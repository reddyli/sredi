package org.sredi.storage;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sredi.commands.Command;
import org.sredi.commands.CommandConstructor;
import org.sredi.commands.EofCommand;
import org.sredi.commands.TerminateCommand;
import org.sredi.constants.ReplicationConstants;
import org.sredi.replication.ClientConnection;
import org.sredi.replication.ConnectionManager;
import org.sredi.replication.FollowerService;
import org.sredi.replication.LeaderService;
import org.sredi.replication.ReplicationServiceInfoProvider;
import org.sredi.resp.RespConstants;
import org.sredi.resp.RespSimpleStringValue;
import org.sredi.resp.RespValue;
import org.sredi.resp.RespValueParser;
import org.sredi.setup.SetupOptions;
import org.sredi.streams.IllegalStreamItemIdException;
import org.sredi.streams.StreamData;
import org.sredi.streams.StreamId;
import org.sredi.streams.StreamValue;
import org.sredi.streams.StreamsWaitManager;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Core data repository that manages key-value storage, client connections, and command execution.
 * This is an abstract class with concrete implementations for Leader and Follower roles.
 */

public abstract class CentralRepository implements ReplicationServiceInfoProvider {
    private static final Logger log = LoggerFactory.getLogger(CentralRepository.class);

    private static final Set<String> DEFAULT_INFO_SECTIONS = Set.of("server", "replication", "stats", "replication-graph");
    private static final int CONNECTION_THREAD_POOL_SIZE = 2;

    // Server socket and event loop for accepting and processing client requests
    private ServerSocket serverSocket;
    private EventLoop eventLoop;
    private final CommandConstructor commandConstructor;
    private final RespValueParser valueParser;

    // Thread pools for handling connections and executing blocking commands
    private final ExecutorService connectionsExecutorService;
    private final ExecutorService commandsExecutorService;

    // Manages all active client connections
    @Getter
    private final ConnectionManager connectionManager;
    private volatile boolean shutdownRequested = false;

    // Server configuration and identity
    private final SetupOptions options;
    @Getter
    private final int port;
    private final String role;
    protected final Clock clock;

    // In-memory key-value data store
    private final Map<String, StoredData> dataStore = new ConcurrentHashMap<>();

    // Handles MULTI/EXEC transaction queuing per connection
    private final TransactionManager transactionManager;
    private ClientConnection currentConnection;
    private final Object connectionLock = new Object();


    // Creates a LeaderService or FollowerService based on the configured role
    public static CentralRepository newInstance(SetupOptions options, Clock clock) {
        String role = options.getRole();
        return switch (role) {
            case ReplicationConstants.REPLICA -> new FollowerService(options, clock);
            case ReplicationConstants.MASTER -> new LeaderService(options, clock);
            default -> throw new UnsupportedOperationException(
                    "Unexpected role type for new repository: " + role);
        };
    }

    // Initializes thread pools, connection manager, and loads any persisted data
    protected CentralRepository(SetupOptions options, Clock clock) {
        this.options = options;
        this.port = options.getPort();
        this.role = options.getRole();
        this.clock = clock;
        this.commandConstructor = new CommandConstructor();
        this.valueParser = new RespValueParser();
        this.connectionManager = new ConnectionManager();

        this.connectionsExecutorService = Executors.newFixedThreadPool(CONNECTION_THREAD_POOL_SIZE);
        this.commandsExecutorService = Executors.newCachedThreadPool();

        this.transactionManager = new TransactionManager(
                this::getCurrentConnection,
                cmd -> cmd.execute(this)
        );

        loadDatabaseFromFile();
    }

    // Loads persisted data from RDB file if configured
    private void loadDatabaseFromFile() {
        if (options.getDbfilename() == null) {
            return;
        }

        Path dbPath = Path.of(options.getDir(), options.getDbfilename());
        if (!Files.exists(dbPath)) {
            log.warn("Database file {} does not exist", dbPath.toAbsolutePath());
            return;
        }

        try {
            DatabaseReader reader = new DatabaseReader(dbPath.toFile(), dataStore, clock);
            reader.readDatabase();
        } catch (IOException e) {
            throw new RuntimeException("Failed to load database file: " + dbPath, e);
        }
    }


    // Opens the server socket and starts accepting client connections
    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        serverSocket.setReuseAddress(true);
        log.info("Server started. Listening on Port {}", port);

        eventLoop = new EventLoop(this, commandConstructor);
        startAcceptThread();
        connectionManager.start(connectionsExecutorService);
    }

    // Runs a background thread that accepts incoming socket connections
    private void startAcceptThread() {
        connectionsExecutorService.execute(() -> {
            while (!shutdownRequested) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    configureSocket(clientSocket);
                    ClientConnection conn = new ClientConnection(clientSocket, valueParser);
                    connectionManager.addConnection(conn);
                    log.debug("Connection accepted from client: {}", conn);
                } catch (IOException e) {
                    if (!shutdownRequested) {
                        log.error("IOException on accept: {}", e.getMessage());
                    }
                }
            }
            connectionManager.closeAllConnections();
        });
    }

    // Sets TCP options for low latency and persistent connections
    private void configureSocket(Socket socket) throws IOException {
        socket.setTcpNoDelay(true);
        socket.setKeepAlive(true);
        socket.setSoTimeout(0);
    }

    // Main loop that processes commands from all connected clients
    public void runCommandLoop() throws InterruptedException {
        eventLoop.runCommandLoop();
    }

    // Gracefully shuts down the server and closes all connections
    public void terminate() {
        log.info("Terminate invoked. Closing {} connections.", connectionManager.getNumConnections());
        eventLoop.terminate();
        shutdownRequested = true;
        try {
            serverSocket.close();
        } catch (IOException e) {
            log.error("IOException on socket close: {}", e.getMessage());
        }
        shutdown();
    }

    // Shuts down thread pools without waiting for completion
    public void shutdown() {
        connectionsExecutorService.shutdown();
        commandsExecutorService.shutdown();
    }

    // Returns a configuration value by name (e.g., dir, dbfilename)
    public String getConfig(String configName) {
        return options.getConfigValue(configName);
    }

    // Checks if a key exists in the data store
    public boolean containsKey(String key) {
        return dataStore.containsKey(key);
    }

    // Checks if a key exists and has not expired
    public boolean containsUnexpiredKey(String key) {
        StoredData storedData = dataStore.get(key);
        return storedData != null && !isExpired(storedData);
    }

    // Retrieves the stored data for a key
    public StoredData get(String key) {
        return dataStore.get(key);
    }

    // Stores data for a key, returning any previous value
    public StoredData set(String key, StoredData storedData) {
        return dataStore.put(key, storedData);
    }

    // Removes a key from the data store
    public void delete(String key) {
        dataStore.remove(key);
    }

    // Returns all keys in the data store
    public Collection<String> getKeys() {
        return dataStore.keySet();
    }

    // Returns the type of value stored at a key (string, list, stream, etc.)
    public RespSimpleStringValue getType(String key) {
        if (dataStore.containsKey(key)) {
            return dataStore.get(key).getType().getTypeResponse();
        }
        return new RespSimpleStringValue("none");
    }

    // Checks if the stored data has passed its expiration time
    public boolean isExpired(StoredData storedData) {
        return storedData.isExpired(clock.millis());
    }

    // Returns the current time in milliseconds
    public long getCurrentTime() {
        return clock.millis();
    }

    // Appends an entry to a stream, creating the stream if it doesn't exist
    public StreamId xadd(String key, String itemId, RespValue[] itemMap)
            throws IllegalStreamItemIdException {
        StoredData storedData = getOrCreateStreamData(key);
        return storedData.getStreamValue().add(itemId, clock, itemMap);
    }

    // Returns entries from a stream within the given ID range
    public List<StreamValue> xrange(String key, String start, String end)
            throws IllegalStreamItemIdException {
        StoredData storedData = getOrCreateStreamData(key);
        return storedData.getStreamValue().queryRange(start, end);
    }

    // Reads from multiple streams, optionally blocking until new data arrives
    public List<List<StreamValue>> xread(
            List<String> keys, List<String> startValues, Long timeoutMillis)
            throws IllegalStreamItemIdException {
        Map<String, StreamData> streams = keys.stream()
                .collect(Collectors.toMap(
                        k -> k,
                        k -> getOrCreateStreamData(k).getStreamValue()));

        Map<String, StreamId> startIds = new HashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            startIds.put(key, streams.get(key).getStreamIdForRead(startValues.get(i)));
        }

        Map<String, List<StreamValue>> values = StreamsWaitManager.INSTANCE.readWithWait(
                streams, startIds, 0, clock, timeoutMillis == null ? 1L : timeoutMillis);
        return keys.stream().map(values::get).toList();
    }

    // Gets or creates a stream data structure for the given key
    private StoredData getOrCreateStreamData(String key) {
        return dataStore.computeIfAbsent(key,
                k -> new StoredData(new StreamData(k), clock.millis(), null));
    }


    // Subclasses implement this to handle command execution and replication
    public abstract void execute(Command command, ClientConnection conn) throws IOException;

    // Executes a command using the current connection context
    public void execute(Command command) throws IOException {
        ClientConnection conn = getCurrentConnection();
        if (conn == null) {
            throw new IllegalStateException("No active connection");
        }
        execute(command, conn);
    }

    // Dispatches command execution, using a separate thread for blocking commands
    void executeCommand(ClientConnection conn, Command command) throws IOException {
        log.debug("Received client command: {}", command);
        setCurrentConnection(conn);

        try {
            if (command.isBlockingCommand()) {
                commandsExecutorService.submit(() -> {
                    try {
                        execute(command, conn);
                    } catch (Exception e) {
                        log.error("Blocking command exception: {} \"{}\"",
                                e.getClass().getSimpleName(), e.getMessage(), e);
                    }
                });
            } else {
                execute(command, conn);
            }

            handleSpecialCommands(conn, command);
        } finally {
            setCurrentConnection(null);
        }
    }

    // Handles EOF and terminate commands after normal execution
    private void handleSpecialCommands(ClientConnection conn, Command command) throws IOException {
        switch (command) {
            case EofCommand ignored -> conn.close();
            case TerminateCommand ignored -> terminate();
            default -> { }
        }
    }

    // Builds the INFO command response based on requested sections
    public String info(Map<String, RespValue> optionsMap) {
        StringBuilder sb = new StringBuilder();

        if (shouldIncludeSection(optionsMap, "server")) {
            sb.append("# Server info\n");
            sb.append("version:").append("\n");
        }

        if (shouldIncludeSection(optionsMap, "replication")) {
            sb.append("# Replication\n");
            sb.append("role:").append(role).append("\n");
            getReplicationInfo(sb);
        }

        return sb.toString();
    }

    // Determines if a section should be included in the INFO response
    private boolean shouldIncludeSection(Map<String, RespValue> optionsMap, String section) {
        return optionsMap.containsKey("all")
                || optionsMap.containsKey("everything")
                || (optionsMap.size() == 1 && DEFAULT_INFO_SECTIONS.contains(section))
                || (optionsMap.containsKey("default") && DEFAULT_INFO_SECTIONS.contains(section))
                || optionsMap.containsKey(section);
    }

    // Handles REPLCONF ACK from followers to track replication progress
    public abstract byte[] replicationConfirm(ClientConnection connection, Map<String, RespValue> optionsMap,
            long startBytesOffset);

    // Blocks until the specified number of replicas have acknowledged writes
    public abstract int waitForReplicationServers(int numReplicas, long timeoutMillis);

    // Initiates partial resynchronization with a replica
    public byte[] psync(Map<String, RespValue> optionsMap) {
        return RespConstants.OK;
    }

    // Sends the RDB snapshot during full resynchronization
    public byte[] psyncRdb(Map<String, RespValue> optionsMap) {
        throw new UnsupportedOperationException("No psync RDB implementation for this repository");
    }

    // Begins a new transaction for the current connection
    public void startTransaction() {
        transactionManager.startTransaction();
    }

    // Adds a command to the current transaction queue
    public void queueCommand(Command command) {
        transactionManager.queueCommand(command);
    }

    // Executes all queued commands and returns their results
    public byte[][] executeTransaction() {
        return transactionManager.executeTransaction();
    }

    // Discards all queued commands without executing them
    public void discardTransaction() {
        transactionManager.discardTransaction();
    }

    // Checks if the given connection has an active transaction
    public boolean hasActiveTransaction(ClientConnection conn) {
        return transactionManager.hasActiveTransaction(conn);
    }

    // Returns true if the command should be queued during a transaction
    protected boolean isQueueableCommand(Command command) {
        Command.Type type = command.getType();
        return type != Command.Type.MULTI && type != Command.Type.EXEC && type != Command.Type.DISCARD;
    }

    // Sets the connection context for the current command execution
    private void setCurrentConnection(ClientConnection conn) {
        synchronized (connectionLock) {
            currentConnection = conn;
        }
    }

    // Returns the connection associated with the current command
    private ClientConnection getCurrentConnection() {
        synchronized (connectionLock) {
            return currentConnection;
        }
    }
}
