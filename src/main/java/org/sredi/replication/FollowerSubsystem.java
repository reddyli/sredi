package org.sredi.replication;

import java.io.IOException;
import java.net.Socket;
import java.util.Map;
import java.util.function.BooleanSupplier;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sredi.commands.Command;
import org.sredi.commands.ReplConfCommand;
import org.sredi.resp.RespArrayValue;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespConstants;
import org.sredi.resp.RespValue;

/**
 * Pluggable subsystem encapsulating follower-only replication state and behavior.
 * Owns the socket and {@link ConnectionToLeader} that read commands from the leader.
 * Designed to be started and stopped at runtime so a node can transition
 * into and out of the follower role without restarting the JVM.
 */
public class FollowerSubsystem {
    private static final Logger log = LoggerFactory.getLogger(FollowerSubsystem.class);
    private static final int MAX_RECONNECT_DELAY_MS = 30_000;
    private static final int INITIAL_RECONNECT_DELAY_MS = 1_000;

    @Getter
    private final ConnectionManager connectionManager;

    @Getter
    private final int port;

    @Getter
    private final String leaderHost;

    @Getter
    private final int leaderPort;

    private final BooleanSupplier shutdownRequested;

    @Getter
    private Socket leaderClientSocket;

    @Getter
    private ConnectionToLeader leaderConnection;

    private volatile boolean stopped = false;

    public FollowerSubsystem(ConnectionManager connectionManager, int port,
            String leaderHost, int leaderPort, BooleanSupplier shutdownRequested) {
        this.connectionManager = connectionManager;
        this.port = port;
        this.leaderHost = leaderHost;
        this.leaderPort = leaderPort;
        this.shutdownRequested = shutdownRequested;
    }

    // Connects to the leader and starts the replication handshake
    public void start() throws IOException {
        if (stopped) {
            throw new IllegalStateException("Subsystem already stopped");
        }
        connectToLeader();
        log.info("Follower subsystem started, leader={}:{}", leaderHost, leaderPort);
    }

    private void connectToLeader() throws IOException {
        leaderClientSocket = new Socket(leaderHost, leaderPort);
        leaderClientSocket.setReuseAddress(true);
        leaderConnection = new ConnectionToLeader(this);
        leaderConnection.startHandshake();
    }

    // Tears down the leader connection and prevents further reconnects
    public void stop() {
        if (stopped) return;
        stopped = true;
        if (leaderConnection != null) {
            leaderConnection.terminate();
        }
        log.info("Follower subsystem stopped");
    }

    public boolean isStopped() {
        return stopped;
    }

    // Reconnects to leader with exponential backoff, exits when stopped or shut down
    public void reconnectToLeader() {
        if (stopped) return;
        Thread reconnectThread = new Thread(() -> {
            int delay = INITIAL_RECONNECT_DELAY_MS;
            while (!stopped && !shutdownRequested.getAsBoolean()) {
                try {
                    log.info("Attempting to reconnect to leader in {}ms...", delay);
                    Thread.sleep(delay);
                    if (stopped || shutdownRequested.getAsBoolean()) return;
                    connectToLeader();
                    log.info("Reconnected to leader successfully");
                    return;
                } catch (IOException e) {
                    log.warn("Reconnection failed: {}", e.getMessage());
                    delay = Math.min(delay * 2, MAX_RECONNECT_DELAY_MS);
                } catch (InterruptedException e) {
                    log.info("Reconnection interrupted");
                    return;
                }
            }
        });
        reconnectThread.setDaemon(true);
        reconnectThread.setName("follower-reconnect");
        reconnectThread.start();
    }

    // Returns true if the given client connection is the one to the leader
    public boolean isLeaderConnection(ClientConnection conn) {
        return leaderConnection != null && leaderConnection.isLeaderConnection(conn);
    }

    // Handles REPLCONF GETACK from leader, responding with bytes received post-handshake
    public byte[] replicationConfirm(ClientConnection connection, Map<String, RespValue> optionsMap,
            long startBytesOffset) {
        if (optionsMap.containsKey(ReplConfCommand.GETACK_NAME)) {
            long replicationOffset = startBytesOffset - leaderConnection.getHandshakeBytesReceived();
            return new RespArrayValue(new RespValue[] {
                    new RespBulkString(Command.Type.REPLCONF.name().getBytes()),
                    new RespBulkString("ACK".getBytes()),
                    new RespBulkString(String.valueOf(replicationOffset).getBytes())
            }).asResponse();
        }
        return RespConstants.OK;
    }
}
