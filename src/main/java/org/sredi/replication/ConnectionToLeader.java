package org.sredi.replication;

import java.io.IOException;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sredi.commands.Command;
import org.sredi.commands.PingCommand;
import org.sredi.commands.PsyncCommand;
import org.sredi.commands.ReplConfCommand;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespValue;
import org.sredi.resp.RespValueParser;

/**
 * Represents the follower's connection to its leader server.
 * Handles the replication handshake sequence (PING -> REPLCONF -> PSYNC)
 * and processes commands received from the leader after handshake completes.
 *
 * Handshake sequence:
 * 1. PING - verify connectivity
 * 2. REPLCONF listening-port - tell leader our port
 * 3. REPLCONF capa psync2 - advertise capabilities
 * 4. PSYNC ? -1 - request full resync (first time) or partial resync
 * 5. Receive FULLRESYNC response + RDB snapshot
 */
public class ConnectionToLeader {

    private static final Logger log = LoggerFactory.getLogger(ConnectionToLeader.class);
    private static final long HANDSHAKE_POLL_INTERVAL_MS = 50L;

    private final FollowerService service;
    private final ClientConnection leaderConnection;
    private final ExecutorService executor;

    // Queue of commands to send during handshake (processed sequentially)
    private final Deque<HandshakeStep> pendingHandshakeSteps = new ConcurrentLinkedDeque<>();

    // Bytes received during handshake - used to calculate replication offset
    @Getter
    private long handshakeBytesReceived = 0;

    // RDB snapshot received during FULLRESYNC
    private RespBulkString fullResyncRdb;

    private volatile boolean done = false;

    public ConnectionToLeader(FollowerService service) throws IOException {
        this.service = service;
        this.executor = Executors.newSingleThreadExecutor();

        RespValueParser valueParser = new RespValueParser();
        this.leaderConnection = new ClientConnection(service.getLeaderClientSocket(), valueParser);
        log.info("Connection to leader: {}, isOpened: {}", leaderConnection, !leaderConnection.isClosed());

        startHandshakeThread();
    }

    // Starts background thread that processes handshake commands
    private void startHandshakeThread() {
        executor.execute(() -> {
            try {
                runHandshakeLoop();
            } catch (InterruptedException e) {
                log.error("Handshake thread interrupted: {}", e.getMessage());
                terminate();
            }
        });
    }

    // Initiates the handshake sequence with the leader
    public void startHandshake() {
        log.info("Starting handshake with leader");
        queueHandshakeStep(new PingCommand(), this::onPingResponse);
    }

    // Step 1: After PING response, send REPLCONF listening-port
    private boolean onPingResponse(Command cmd, RespValue response) {
        ReplConfCommand portConf = new ReplConfCommand(
                ReplConfCommand.Option.LISTENING_PORT,
                String.valueOf(service.getPort()));
        queueHandshakeStep(portConf, this::onListeningPortResponse);
        return false;
    }

    // Step 2: After REPLCONF listening-port, send REPLCONF capa psync2
    private boolean onListeningPortResponse(Command cmd, RespValue response) {
        ReplConfCommand capaConf = new ReplConfCommand(ReplConfCommand.Option.CAPA, "psync2");
        queueHandshakeStep(capaConf, this::onCapaResponse);
        return false;
    }

    // Step 3: After REPLCONF capa, send PSYNC ? -1
    private boolean onCapaResponse(Command cmd, RespValue response) {
        PsyncCommand psync = new PsyncCommand("?", -1L);
        queueHandshakeStep(psync, this::onPsyncResponse);
        return false;
    }

    // Step 4: Handle PSYNC response - expect FULLRESYNC followed by RDB
    private boolean onPsyncResponse(Command cmd, RespValue response) {
        if (response.isSimpleString() && response.getValueAsString().toUpperCase().startsWith("FULLRESYNC")) {
            log.debug("Full resync - expecting RDB snapshot next");
            return true; // Signal that we expect RDB data next
        }

        if (!response.isBulkString()) {
            throw new RuntimeException("Unexpected PSYNC response: " + response);
        }

        completeHandshake((RespBulkString) response);
        return false;
    }

    // Called when RDB snapshot is received, completing the handshake
    private void completeHandshake(RespBulkString rdbData) {
        this.fullResyncRdb = rdbData;
        this.handshakeBytesReceived = leaderConnection.getNumBytesReceived();
        log.info("Handshake completed, received {} bytes during handshake", handshakeBytesReceived);

        // Register with ConnectionManager to receive replicated commands
        service.getConnectionManager().addPriorityConnection(leaderConnection);
    }

    // Adds a command to the handshake queue
    private void queueHandshakeStep(Command command, BiFunction<Command, RespValue, Boolean> responseHandler) {
        pendingHandshakeSteps.offerLast(new HandshakeStep(command, responseHandler));
    }

    // Returns the RDB snapshot received during full resync
    public byte[] getFullResyncRdb() {
        return fullResyncRdb != null ? fullResyncRdb.getValue() : null;
    }

    // Closes connection to leader and shuts down executor
    public void terminate() {
        log.info("Terminating connection to leader: {}", service.getLeaderClientSocket());
        done = true;
        try {
            service.getLeaderClientSocket().close();
        } catch (IOException e) {
            log.error("Error closing socket to leader: {}", e.getMessage());
        }
        executor.close();
    }

    // Main loop that processes handshake commands sequentially
    public void runHandshakeLoop() throws InterruptedException {
        while (!done) {
            if (leaderConnection.isClosed()) {
                log.warn("Leader closed connection");
                terminate();
                service.reconnectToLeader();
                return;
            }

            processNextHandshakeStep();
            Thread.sleep(HANDSHAKE_POLL_INTERVAL_MS);
        }
        log.debug("Handshake loop exited");
    }

    // Processes all pending handshake steps
    private void processNextHandshakeStep() {
        try {
            while (!pendingHandshakeSteps.isEmpty()) {
                HandshakeStep step = pendingHandshakeSteps.pollFirst();
                executeHandshakeStep(step);
            }
        } catch (Exception e) {
            log.error("Error in handshake loop: {} \"{}\"", e.getClass().getSimpleName(), e.getMessage());
        }
    }

    // Sends command to leader and processes response
    private void executeHandshakeStep(HandshakeStep step) throws IOException {
        log.debug("Sending handshake command: {}", step.command);
        leaderConnection.writeFlush(step.command.asCommand());

        RespValue response = leaderConnection.readValue();
        log.debug("Received response: {}", response);

        // If handler returns true, we expect RDB data next
        if (step.responseHandler.apply(step.command, response)) {
            readAndProcessRdb(step);
        }
    }

    // Reads RDB snapshot after FULLRESYNC response
    private void readAndProcessRdb(HandshakeStep step) {
        try {
            byte[] rdb = leaderConnection.readRDB();
            RespBulkString rdbValue = new RespBulkString(rdb);
            log.debug("Received RDB snapshot: {} bytes", rdb.length);
            step.responseHandler.apply(step.command, rdbValue);
        } catch (IOException e) {
            log.error("Failed to read RDB: {} {}", e.getClass().getSimpleName(), e.getMessage());
        }
    }

    // Checks if the given connection is the leader connection
    public boolean isLeaderConnection(ClientConnection conn) {
        return leaderConnection.equals(conn);
    }

    // Executes a command received from the leader (after handshake)
    public void executeCommandFromLeader(ClientConnection conn, Command command) throws IOException {
        if (!isLeaderConnection(conn)) {
            log.error("executeCommandFromLeader called with non-leader connection: {}", conn);
            return;
        }

        logReceivedCommand(command);

        byte[] response = command.execute(service);

        if (shouldSendResponseToLeader(command)) {
            sendResponseToLeader(conn, command, response);
        } else {
            log.trace("Not sending response for {}: {}", command.getType().name(), Command.responseLogString(response));
        }
    }

    private void logReceivedCommand(Command command) {
        if (command.isReplicatedCommand()) {
            log.debug("Received replicated command from leader");
        } else {
            log.debug("Received request from leader");
        }
    }

    private void sendResponseToLeader(ClientConnection conn, Command command, byte[] response) throws IOException {
        log.debug("Sending {} response: {}", command.getType().name(), Command.responseLogString(response));
        if (response != null && response.length > 0) {
            conn.writeFlush(response);
        }
    }

    // Determines if we should respond to the leader for this command
    // Only REPLCONF GETACK requires a response; replicated writes don't
    public boolean shouldSendResponseToLeader(Command command) {
        if (command instanceof ReplConfCommand replConf) {
            return replConf.getOptionsMap().containsKey(ReplConfCommand.GETACK_NAME);
        }
        return false;
    }

    // Pairs a handshake command with its response handler
    private record HandshakeStep(
            Command command,
            BiFunction<Command, RespValue, Boolean> responseHandler
    ) {}
}
