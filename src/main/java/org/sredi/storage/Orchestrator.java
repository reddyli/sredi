package org.sredi.storage;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sredi.commands.Command;
import org.sredi.commands.CommandConstructor;
import org.sredi.commands.EofCommand;
import org.sredi.commands.PsyncCommand;
import org.sredi.commands.TerminateCommand;
import org.sredi.constants.ReplicationConstants;
import org.sredi.replication.ClientConnection;
import org.sredi.replication.ConnectionManager;
import org.sredi.replication.FollowerSubsystem;
import org.sredi.replication.LeaderSubsystem;
import org.sredi.replication.ReplicationServiceInfoProvider;
import org.sredi.resp.RespConstants;
import org.sredi.resp.RespSimpleStringValue;
import org.sredi.resp.RespValue;
import org.sredi.resp.RespValueParser;
import org.sredi.setup.SetupOptions;
import org.sredi.streams.IllegalStreamItemIdException;
import org.sredi.streams.StreamId;
import org.sredi.streams.StreamValue;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.*;

/**
 * Core server orchestrator that manages client connections, command execution, and replication.
 * Data operations are delegated to {@link DataStore}.
 * Holds a {@link LeaderSubsystem} or {@link FollowerSubsystem} depending on the current role,
 * and exposes {@link #becomeLeader()} / {@link #becomeFollowerOf(String, int)} so the role
 * can change at runtime (e.g. after a leader election).
 */

public class Orchestrator implements ReplicationServiceInfoProvider {
    private static final Logger log = LoggerFactory.getLogger(Orchestrator.class);

    private static final Set<String> DEFAULT_INFO_SECTIONS = Set.of("server", "replication", "stats", "replication-graph");
    private static final int CONNECTION_THREAD_POOL_SIZE = 2;

    public enum Role { LEADER, FOLLOWER }

    // Server socket and event loop for accepting and processing client requests
    private ServerSocket serverSocket;
    private EventLoop eventLoop;
    private final CommandConstructor commandConstructor;
    private final RespValueParser valueParser;

    // Thread pools for handling connections and executing blocking commands
    private final ExecutorService connectionsExecutorService;
    private final ExecutorService commandsExecutorService;
    private final ScheduledExecutorService cleanupExecutorService;

    // Manages all active client connections
    @Getter
    private final ConnectionManager connectionManager;
    @Getter
    private volatile boolean shutdownRequested = false;

    // Server configuration and identity
    @Getter
    private final SetupOptions options;
    @Getter
    private final int port;
    protected final Clock clock;

    // Current role and the subsystem that implements its replication behavior.
    // Exactly one of leaderSubsystem / followerSubsystem is non-null at a time.
    private volatile Role role;
    private volatile LeaderSubsystem leaderSubsystem;
    private volatile FollowerSubsystem followerSubsystem;
    private final Object roleLock = new Object();

    // In-memory data store with LRU and eviction
    @Getter
    private final DataStore dataStore;

    // Handles MULTI/EXEC transaction queuing per connection
    private final TransactionManager transactionManager;
    // Handles pub/sub channel subscriptions and message delivery
    private final PubSubManager pubSubManager;
    private ClientConnection currentConnection;
    private final Object connectionLock = new Object();


    // Factory retained for backwards compatibility; the Orchestrator constructor
    // now decides the initial role itself based on SetupOptions.
    public static Orchestrator newInstance(SetupOptions options, Clock clock) {
        return new Orchestrator(options, clock);
    }

    // Initializes thread pools, connection manager, the initial role subsystem,
    // and loads any persisted data
    public Orchestrator(SetupOptions options, Clock clock) {
        this.options = options;
        this.port = options.getPort();
        this.clock = clock;
        this.commandConstructor = new CommandConstructor();
        this.valueParser = new RespValueParser();
        this.connectionManager = new ConnectionManager(options.getMaxClients());

        this.dataStore = new DataStore(clock, options.getMaxKeys());

        this.connectionsExecutorService = Executors.newFixedThreadPool(CONNECTION_THREAD_POOL_SIZE);
        this.commandsExecutorService = Executors.newCachedThreadPool();
        this.cleanupExecutorService = Executors.newScheduledThreadPool(1);

        this.transactionManager = new TransactionManager(
                this::getCurrentConnection,
                cmd -> cmd.execute(this)
        );
        this.pubSubManager = new PubSubManager(this::getCurrentConnection);
        this.connectionManager.setOnConnectionClosed(pubSubManager::removeConnection);

        if (ReplicationConstants.REPLICA.equals(options.getRole())) {
            this.role = Role.FOLLOWER;
            this.followerSubsystem = newFollowerSubsystem(options.getReplicaof(), options.getReplicaofPort());
        } else {
            this.role = Role.LEADER;
            this.leaderSubsystem = newLeaderSubsystem();
        }

        loadDatabaseFromFile();
    }

    private LeaderSubsystem newLeaderSubsystem() {
        return new LeaderSubsystem(connectionManager, clock, options.getMaxReplBacklog());
    }

    private FollowerSubsystem newFollowerSubsystem(String leaderHost, int leaderPort) {
        return new FollowerSubsystem(connectionManager, port, leaderHost, leaderPort, this::isShutdownRequested);
    }

    public Role getRole() {
        return role;
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
            DatabaseReader reader = new DatabaseReader(dbPath.toFile(), dataStore.getEntries(), clock);
            reader.readDatabase();
        } catch (IOException e) {
            throw new RuntimeException("Failed to load database file: " + dbPath, e);
        }
    }


    // Opens the server socket, starts accepting client connections, and starts
    // the role-specific subsystem (leader or follower)
    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        serverSocket.setReuseAddress(true);
        log.info("Server started. Listening on Port {}", port);

        eventLoop = new EventLoop(this, commandConstructor);
        startAcceptThread();
        connectionManager.start(connectionsExecutorService);

        // Background cleanup of expired keys
        cleanupExecutorService.scheduleAtFixedRate(
                dataStore::cleanupExpiredKeys, 10, 30, TimeUnit.SECONDS);

        if (leaderSubsystem != null) {
            leaderSubsystem.start();
        } else if (followerSubsystem != null) {
            followerSubsystem.start();
        }
    }

    // Runs a background thread that accepts incoming socket connections
    private void startAcceptThread() {
        connectionsExecutorService.execute(() -> {
            while (!shutdownRequested) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    configureSocket(clientSocket);
                    ClientConnection conn = new ClientConnection(clientSocket, valueParser);
                    if (options.getMaxRps() > 0) {
                        conn.setRateLimiter(new RateLimiter(options.getMaxRps()));
                    }
                    if (!connectionManager.addConnection(conn)) {
                        log.warn("Max clients reached, rejecting connection");
                        conn.close();
                        continue;
                    }
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
        if (leaderSubsystem != null) leaderSubsystem.stop();
        if (followerSubsystem != null) followerSubsystem.stop();
        connectionsExecutorService.shutdown();
        commandsExecutorService.shutdown();
        cleanupExecutorService.shutdown();
    }

    // Promotes this node to the leader role. Tears down any active follower
    // subsystem and starts a fresh leader subsystem.
    public void becomeLeader() {
        synchronized (roleLock) {
            if (role == Role.LEADER) return;
            tearDownFollower();
            this.leaderSubsystem = newLeaderSubsystem();
            this.leaderSubsystem.start();
            this.role = Role.LEADER;
            log.info("Promoted to LEADER");
        }
    }

    // Demotes this node to the follower role and connects to the given leader.
    // Tears down any active leader subsystem and starts a fresh follower subsystem.
    public void becomeFollowerOf(String leaderHost, int leaderPort) throws IOException {
        synchronized (roleLock) {
            tearDownLeader();
            FollowerSubsystem next = newFollowerSubsystem(leaderHost, leaderPort);
            this.followerSubsystem = next;
            this.role = Role.FOLLOWER;
            next.start();
            log.info("Following leader {}:{}", leaderHost, leaderPort);
        }
    }

    private void tearDownLeader() {
        if (leaderSubsystem != null) {
            leaderSubsystem.stop();
            leaderSubsystem = null;
        }
    }

    private void tearDownFollower() {
        if (followerSubsystem != null) {
            followerSubsystem.stop();
            followerSubsystem = null;
        }
    }

    // Returns a configuration value by name (e.g., dir, dbfilename)
    public String getConfig(String configName) {
        return options.getConfigValue(configName);
    }

    // Delegate data operations to DataStore

    public boolean containsKey(String key) { return dataStore.containsKey(key); }
    public boolean containsUnexpiredKey(String key) { return dataStore.containsUnexpiredKey(key); }
    public DataEntry get(String key) { return dataStore.get(key); }
    public DataEntry set(String key, DataEntry entry) { return dataStore.set(key, entry); }
    public void delete(String key) { dataStore.delete(key); }
    public Collection<String> getKeys() { return dataStore.getKeys(); }
    public RespSimpleStringValue getType(String key) { return dataStore.getType(key); }
    public boolean isExpired(DataEntry entry) { return dataStore.isExpired(entry); }
    public long getCurrentTime() { return dataStore.getCurrentTime(); }

    public StreamId xadd(String key, String itemId, RespValue[] itemMap)
            throws IllegalStreamItemIdException { return dataStore.xadd(key, itemId, itemMap); }
    public List<StreamValue> xrange(String key, String start, String end)
            throws IllegalStreamItemIdException { return dataStore.xrange(key, start, end); }
    public List<List<StreamValue>> xread(List<String> keys, List<String> startValues)
            throws IllegalStreamItemIdException { return dataStore.xread(keys, startValues); }

    public long lpush(String key, String value) { return dataStore.lpush(key, value); }
    public long rpush(String key, String value) { return dataStore.rpush(key, value); }
    public String lpop(String key) { return dataStore.lpop(key); }
    public String rpop(String key) { return dataStore.rpop(key); }
    public List<String> lrange(String key, int start, int end) { return dataStore.lrange(key, start, end); }

    public void bfReserve(String key, long capacity, double errorRate) { dataStore.bfReserve(key, capacity, errorRate); }
    public BloomFilter bfGetOrCreate(String key, long capacity, double errorRate) { return dataStore.bfGetOrCreate(key, capacity, errorRate); }
    public BloomFilter bfGet(String key) { return dataStore.bfGet(key); }

    // Handles command execution, dispatching based on the current role.
    // Leader: runs the command, replies, and replicates writes to followers.
    // Follower: rejects writes from non-leader connections, otherwise runs
    // them locally and only replies to non-leader callers.
    public void execute(Command command, ClientConnection conn) throws IOException {
        if (role == Role.LEADER) {
            executeAsLeader(command, conn);
        } else {
            executeAsFollower(command, conn);
        }
    }

    private void executeAsLeader(Command command, ClientConnection conn) throws IOException {
        if (hasActiveTransaction(conn) && isQueueableCommand(command)) {
            queueCommand(command);
            conn.sendResponse(new RespSimpleStringValue("QUEUED").asResponse());
            return;
        }

        byte[] response = command.execute(this);
        if (response != null) {
            conn.sendResponse(response);
        }

        LeaderSubsystem ls = leaderSubsystem;
        if (ls != null) {
            ls.replicate(command);
        }
    }

    private void executeAsFollower(Command command, ClientConnection conn) throws IOException {
        FollowerSubsystem fs = followerSubsystem;
        boolean fromLeader = fs != null && fs.isLeaderConnection(conn);

        if (!fromLeader && command.getType().isWrite()) {
            conn.sendError("READONLY You can't write against a read only replica.");
            return;
        }

        if (hasActiveTransaction(conn) && isQueueableCommand(command)) {
            queueCommand(command);
            conn.sendResponse(new RespSimpleStringValue("QUEUED").asResponse());
            return;
        }

        byte[] response = command.execute(this);
        if (response != null && !fromLeader) {
            conn.sendResponse(response);
        }
    }

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

        // Auth check: reject non-AUTH commands if connection is not authenticated
        if (isAuthRequired() && !conn.isAuthenticated() && command.getType() != Command.Type.AUTH) {
            conn.sendError("NOAUTH Authentication required");
            return;
        }

        // Rate limit check
        if (conn.getRateLimiter() != null && !conn.getRateLimiter().tryConsume()) {
            conn.sendError("ERR rate limit exceeded");
            return;
        }

        // Subscribed mode: restrict commands while the connection has active subscriptions
        if (pubSubManager.isSubscribed(conn) && !isAllowedInSubscribedMode(command)) {
            conn.sendError("ERR Can't execute '" + command.getType()
                    + "': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING allowed in this context");
            return;
        }

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

    // Handles post-execution actions for special commands
    private void handleSpecialCommands(ClientConnection conn, Command command) throws IOException {
        switch (command) {
            case EofCommand ignored -> conn.close();
            case TerminateCommand ignored -> terminate();
            case PsyncCommand ignored -> onPsyncComplete(conn);
            default -> { }
        }
    }

    // Called after PSYNC completes; on a leader, registers the follower for replication
    protected void onPsyncComplete(ClientConnection conn) {
        LeaderSubsystem ls = leaderSubsystem;
        if (ls != null) {
            ls.registerFollower(conn);
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
            sb.append("role:").append(roleString()).append("\n");
            getReplicationInfo(sb);
        }

        return sb.toString();
    }

    private String roleString() {
        return role == Role.LEADER ? ReplicationConstants.MASTER : ReplicationConstants.REPLICA;
    }

    @Override
    public void getReplicationInfo(StringBuilder sb) {
        LeaderSubsystem ls = leaderSubsystem;
        if (ls != null) {
            ls.appendReplicationInfo(sb);
        }
    }

    // Determines if a section should be included in the INFO response
    private boolean shouldIncludeSection(Map<String, RespValue> optionsMap, String section) {
        return optionsMap.containsKey("all")
                || optionsMap.containsKey("everything")
                || (optionsMap.size() == 1 && DEFAULT_INFO_SECTIONS.contains(section))
                || (optionsMap.containsKey("default") && DEFAULT_INFO_SECTIONS.contains(section))
                || optionsMap.containsKey(section);
    }

    // Handles REPLCONF GETACK / ACK; routed to the active subsystem
    public byte[] replicationConfirm(ClientConnection connection, Map<String, RespValue> optionsMap,
            long startBytesOffset) {
        LeaderSubsystem ls = leaderSubsystem;
        if (ls != null) {
            return ls.replicationConfirm(connection, optionsMap, startBytesOffset);
        }
        FollowerSubsystem fs = followerSubsystem;
        if (fs != null) {
            return fs.replicationConfirm(connection, optionsMap, startBytesOffset);
        }
        return RespConstants.OK;
    }

    // Blocks until the specified number of replicas have acknowledged writes.
    // Followers always return 0.
    public int waitForReplicationServers(int numReplicas, long timeoutMillis) {
        LeaderSubsystem ls = leaderSubsystem;
        return ls != null ? ls.waitForReplicas(numReplicas, timeoutMillis) : 0;
    }

    // Initiates partial resynchronization with a replica (leader-only)
    public byte[] psync(Map<String, RespValue> optionsMap) {
        LeaderSubsystem ls = leaderSubsystem;
        return ls != null ? ls.psyncResponse() : RespConstants.OK;
    }

    // Sends the RDB snapshot during full resynchronization (leader-only)
    public byte[] psyncRdb(Map<String, RespValue> optionsMap) {
        LeaderSubsystem ls = leaderSubsystem;
        if (ls == null) {
            throw new UnsupportedOperationException("psync RDB not supported for this role");
        }
        return ls.psyncRdb();
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

    // Subscribes the current connection to a channel; returns its total subscription count
    public int subscribe(String channel) {
        return pubSubManager.subscribe(channel);
    }

    // Unsubscribes the current connection from a channel; returns its remaining count
    public int unsubscribe(String channel) {
        return pubSubManager.unsubscribe(channel);
    }

    // Unsubscribes the current connection from all channels; returns the channels removed
    public List<String> unsubscribeAll() {
        return pubSubManager.unsubscribeAll();
    }

    // Delivers a message to all subscribers of a channel; returns delivery count
    public int publish(String channel, byte[] message) {
        return pubSubManager.publish(channel, message);
    }

    // Returns true if the command should be queued during a transaction
    protected boolean isQueueableCommand(Command command) {
        Command.Type type = command.getType();
        return type != Command.Type.MULTI && type != Command.Type.EXEC && type != Command.Type.DISCARD;
    }

    // Returns true if the command is allowed while the connection is in subscribed mode
    private boolean isAllowedInSubscribedMode(Command command) {
        Command.Type type = command.getType();
        return type == Command.Type.SUBSCRIBE
                || type == Command.Type.UNSUBSCRIBE
                || type == Command.Type.PING;
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

    // Checks password and marks current connection as authenticated
    public boolean authenticate(String password) {
        String requiredPassword = options.getPassword();
        if (requiredPassword != null && requiredPassword.equals(password)) {
            getCurrentConnection().setAuthenticated(true);
            return true;
        }
        return false;
    }

    // Returns true if auth is required and current connection is not authenticated
    public boolean isAuthRequired() {
        return options.getPassword() != null;
    }
}
