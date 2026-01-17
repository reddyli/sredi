package org.sredi.storage;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sredi.commands.Command;
import org.sredi.commands.Command.Type;
import org.sredi.commands.EofCommand;
import org.sredi.commands.TerminateCommand;
import org.sredi.constants.ReplicationConstants;
import org.sredi.commands.CommandConstructor;
import org.sredi.replication.*;
import org.sredi.resp.RespConstants;
import org.sredi.resp.RespSimpleStringValue;
import org.sredi.resp.RespValue;
import org.sredi.resp.RespValueParser;
import org.sredi.setup.SetupOptions;
import org.sredi.streams.*;
import org.sredi.streams.StreamData;

public abstract class CentralRepository implements ReplicationServiceInfoProvider {
    private static final Logger log = LoggerFactory.getLogger(CentralRepository.class);

    private static final Set<String> DEFAULT_SECTIONS = Set.of("server", "replication", "stats",
            "replication-graph");

    private ServerSocket serverSocket;
    private EventLoop eventLoop;
    private final CommandConstructor commandConstructor;
    private final RespValueParser valueParser;
    private final ExecutorService connectionsExecutorService;
    private final ExecutorService commandsExecutorService;
    @Getter
    private final ConnectionManager connectionManager;
    private volatile boolean done = false;
    private final SetupOptions options;
    @Getter
    private final int port;
    private final String role;
    protected final Clock clock;
    private final Map<String, StoredData> dataStore = new ConcurrentHashMap<>();
    protected final Map<ClientConnection, List<Command>> transactionQueues = new ConcurrentHashMap<>();

    public static CentralRepository newInstance(SetupOptions options, Clock clock) {
        String role = options.getRole();
        return switch (role) {
            case ReplicationConstants.REPLICA -> new FollowerService(options, clock);
            case ReplicationConstants.MASTER -> new LeaderService(options, clock);
        default -> throw new UnsupportedOperationException(
                "Unexpected role type for new repository: " + role);
        };
    }

    protected CentralRepository(SetupOptions options, Clock clock) {
        this.options = options;
        this.port = options.getPort();
        this.role = options.getRole();
        this.clock = clock;
        commandConstructor = new CommandConstructor();
        valueParser = new RespValueParser();

        connectionsExecutorService = Executors.newFixedThreadPool(2);
        commandsExecutorService = Executors.newCachedThreadPool();

        // read from RDB Dump
        if (options.getDbfilename() != null) {
            try {
                File dbFile = new File(options.getDir(), options.getDbfilename());
                // only read the file if it exists
                if (dbFile.exists()) {
                    DatabaseReader reader = new DatabaseReader(dbFile, dataStore, clock);
                    reader.readDatabase();
                } else {
                    log.warn("Database file {} does not exist", dbFile.getAbsolutePath());
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        connectionManager = new ConnectionManager();
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        serverSocket.setReuseAddress(true);
        log.info("Server started. Listening on Port {}", port);

        eventLoop = new EventLoop(this, commandConstructor);

        // create the thread for accepting new connections
        connectionsExecutorService.execute(() -> {
            while (!done) {
                Socket clientSocket = null;
                try {
                    clientSocket = serverSocket.accept();
                    clientSocket.setTcpNoDelay(true);
                    clientSocket.setKeepAlive(true);
                    clientSocket.setSoTimeout(0); // infinite timeout

                    ClientConnection conn = new ClientConnection(clientSocket, valueParser);
                    connectionManager.addConnection(conn);
                    log.debug("Connection accepted from client: {}, opened: {}", conn, !conn.isClosed());
                } catch (IOException e) {
                    log.error("IOException on accept: {}", e.getMessage());
                }
            }

            // loop was terminated so close any open connections
            connectionManager.closeAllConnections();
        });

        // start thread to read from client connections
        connectionManager.start(connectionsExecutorService);
    }

    public void closeSocket() throws IOException {
        serverSocket.close();
    }

    public String getConfig(String configName) {
        // Note: returns null for unknown config name
        return options.getConfigValue(configName);
    }

    public void shutdown() {
        connectionsExecutorService.shutdown();
        commandsExecutorService.shutdown();
    }

    public boolean containsKey(String key) {
        return dataStore.containsKey(key);
    }

    public boolean containsUnexpiredKey(String key) {
        StoredData storedData = dataStore.getOrDefault(key, null);
        return storedData != null && !isExpired(storedData);
    }

    public StoredData get(String key) {
        return dataStore.get(key);
    }

    public RespSimpleStringValue getType(String key) {
        if (dataStore.containsKey(key)) {
            return dataStore.get(key).getType().getTypeResponse();
        } else {
            return new RespSimpleStringValue("none");
        }
    }

    public StoredData set(String key, StoredData storedData) {
        return dataStore.put(key, storedData);
    }

    public StreamId xadd(String key, String itemId, RespValue[] itemMap)
            throws IllegalStreamItemIdException {
        StoredData storedData = dataStore.computeIfAbsent(key,
                (k) -> new StoredData(new StreamData(k), clock.millis(), null));
        return storedData.getStreamValue().add(itemId, clock, itemMap);
    }

    public List<StreamValue> xrange(String key, String start, String end)
            throws IllegalStreamItemIdException {
        StoredData storedData = dataStore.computeIfAbsent(key,
                (k) -> new StoredData(new StreamData(k), clock.millis(), null));
        return storedData.getStreamValue().queryRange(start, end);
    }

    public List<List<StreamValue>> xread(
            List<String> keys, List<String> startValues, Long timeoutMillis)
            throws IllegalStreamItemIdException {
        Map<String, StreamData> streams = keys.stream()
                .collect(Collectors.toMap(
                        s -> s,
                        s -> dataStore.computeIfAbsent(s,
                                (k) -> new StoredData(new StreamData(k), clock.millis(), null))
                                .getStreamValue()));
        Map<String, StreamId> startIds = new HashMap<>();
        int i = 0;
        for (String s : keys) {
            startIds.put(s, streams.get(s).getStreamIdForRead(startValues.get(i++)));
        }
        Map<String, List<StreamValue>> values = StreamsWaitManager.INSTANCE.readWithWait(streams,
                startIds, 0, clock, timeoutMillis == null ? 1L : timeoutMillis);
        return keys.stream().map(values::get).toList();
    }

    public void delete(String key) {
        dataStore.remove(key);
    }

    public abstract void execute(Command command, ClientConnection conn) throws IOException;

    public void execute(Command command) throws IOException {
        ClientConnection currentConn = getCurrentConnection();
        if (currentConn == null) {
            throw new IllegalStateException("No active connection");
        }
        execute(command, currentConn);
    }

    public boolean isExpired(StoredData storedData) {
        long now = clock.millis();
        return storedData.isExpired(now);
    }

    public long getCurrentTime() {
        return clock.millis();
    }

    public String info(Map<String, RespValue> optionsMap) {

        StringBuilder sb = new StringBuilder();
        if (infoSection(optionsMap, "server")) {
            sb.append("# Server info\n");
            sb.append("version:").append("\n");
        }

        // replication section
        if (infoSection(optionsMap, "replication")) {
            sb.append("# Replication\n");
            sb.append("role:").append(role).append("\n");
            getReplicationInfo(sb);
        }
        return sb.toString();
    }

    private boolean infoSection(Map<String, RespValue> optionsMap, String section) {
        return optionsMap.containsKey("all") || optionsMap.containsKey("everything")
                || (optionsMap.size() == 1 && isDefault(section))
                || (optionsMap.containsKey("default") && isDefault(section))
                // || (optionsMap.containsKey("server") && isServer(section))
                // || (optionsMap.containsKey("clients") && isClients(section))
                // || (optionsMap.containsKey("memory") && isMemory(section))
                || optionsMap.containsKey(section);
    }

    private boolean isDefault(String section) {
        return DEFAULT_SECTIONS.contains(section);
    }

    public abstract byte[] replicationConfirm(ClientConnection connection, Map<String, RespValue> optionsMap,
            long startBytesOffset);

    public abstract int waitForReplicationServers(int numReplicas, long timeoutMillis);

    public byte[] psync(Map<String, RespValue> optionsMap) {
        // TODO make this abstract once leader and follower both override this method
        return RespConstants.OK;
    }

    public byte[] psyncRdb(Map<String, RespValue> optionsMap) {
        // TODO make this abstract once leader and follower both override this method
        throw new UnsupportedOperationException("no psync rdb implementation for the repository");
    }

    public void runCommandLoop() throws InterruptedException {
        eventLoop.runCommandLoop();
    }

    public void terminate() {
        log.info("Terminate invoked. Closing {} connections.", connectionManager.getNumConnections());
        eventLoop.terminate();
        done = true;
        // stop accepting new connections and shut down the accept connections thread
        try {
            closeSocket();
        } catch (IOException e) {
            log.error("IOException on socket close: {}", e.getMessage());
        }
        shutdown();
    }

    void executeCommand(ClientConnection conn, Command command) throws IOException {
        log.debug("Received client command: {}", command);

        // Set the current connection for transaction handling
        setCurrentConnection(conn);

        if (command.isBlockingCommand()) {
            commandsExecutorService.submit(() -> {
                try {
                    execute(command, conn);
                } catch (Exception e) {
                    log.error("EventLoop Exception: {} \"{}\"", e.getClass().getSimpleName(), e.getMessage(), e);
                }
            });
        } else {
            execute(command, conn);
        }

        switch (command) {
        case EofCommand c -> {
            conn.close();
        }
        case TerminateCommand c -> {
            terminate();
        }
        default -> {
            // no action for other command types
        }
        }

        // Clear the current connection
        setCurrentConnection(null);
    }

    private ClientConnection currentConnection;
    private final Object connectionLock = new Object();

    private void setCurrentConnection(ClientConnection conn) {
        synchronized (connectionLock) {
            currentConnection = conn;
        }
    }

    private ClientConnection getCurrentConnection() {
        synchronized (connectionLock) {
            return currentConnection;
        }
    }

    static class EventLoop {
        // keep a list of socket connections and continue checking for new connections
        private final CentralRepository service;
        private final CommandConstructor commandConstructor;
        private volatile boolean done = false;

        public EventLoop(CentralRepository service, CommandConstructor commandConstructor) {
            this.service = service;
            this.commandConstructor = commandConstructor;
        }

        public void terminate() {
            done = true;
        }

        public void runCommandLoop() throws InterruptedException {
            while (!done) {
                // check for a value on one of the client sockets and process it as a command
                boolean didProcess = service.getConnectionManager().getNextValue((conn, value) -> {
                    Command command = commandConstructor.newCommandFromValue(value);
                    if (command != null) {
                        try {
                            service.executeCommand(conn, command);
                        } catch (Exception e) {
                            log.error("EventLoop Exception: {} \"{}\"", e.getClass().getSimpleName(), e.getMessage(), e);
                            // since this is a blocking command, we better return an error response
                            conn.sendError(e.getMessage());
                        }
                    }
                });

                // sleep a bit if there were no commands to be processed
                if (!didProcess) {
                    Thread.sleep(80L);
                }
            }
        }
    }

    public Collection<String> getKeys() {
        return dataStore.keySet();
    }

    public void startTransaction() {
        ClientConnection currentConn = getCurrentConnection();
        if (currentConn == null) {
            throw new IllegalStateException("No active connection for transaction");
        }
        transactionQueues.put(currentConn, new ArrayList<>());
    }

    public void queueCommand(Command command) {
        ClientConnection currentConn = getCurrentConnection();
        if (currentConn == null) {
            throw new IllegalStateException("No active connection for transaction");
        }
        List<Command> queue = transactionQueues.get(currentConn);
        if (queue == null) {
            throw new IllegalStateException("No active transaction");
        }
        queue.add(command);
    }

    public byte[][] executeTransaction() {
        ClientConnection currentConn = getCurrentConnection();
        if (currentConn == null) {
            throw new IllegalStateException("No active connection for transaction");
        }
        List<Command> queue = transactionQueues.remove(currentConn);
        if (queue == null) {
            return null; // No active transaction
        }

        byte[][] results = new byte[queue.size()][];
        for (int i = 0; i < queue.size(); i++) {
            try {
                results[i] = queue.get(i).execute(this);
            } catch (Exception e) {
                // If any command fails, discard the transaction
                transactionQueues.remove(currentConn);
                throw e;
            }
        }
        return results;
    }

    public void discardTransaction() {
        ClientConnection currentConn = getCurrentConnection();
        if (currentConn == null) {
            throw new IllegalStateException("No active connection for transaction");
        }
        transactionQueues.remove(currentConn);
    }

}
