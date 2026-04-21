package org.sredi.replication;

import java.io.IOException;
import java.net.Socket;
import java.time.Clock;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;
import org.sredi.commands.Command;
import org.sredi.commands.ReplConfCommand;
import org.sredi.resp.RespArrayValue;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespConstants;
import org.sredi.resp.RespSimpleStringValue;
import org.sredi.resp.RespValue;
import org.sredi.setup.SetupOptions;
import org.sredi.storage.Orchestrator;

/**
 * Follower (replica) implementation of Orchestrator.
 * Connects to a leader server, performs handshake, and receives replicated commands.
 * Does not propagate commands further - it's a read replica that stays in sync with the leader.
 */
public class FollowerService extends Orchestrator {
    private static final Logger log = LoggerFactory.getLogger(FollowerService.class);
    private static final int MAX_RECONNECT_DELAY_MS = 30_000;
    private static final int INITIAL_RECONNECT_DELAY_MS = 1_000;

    @Getter
    private final String leaderHost;

    @Getter
    private final int leaderPort;

    @Getter
    private Socket leaderClientSocket;

    @Getter
    private ConnectionToLeader leaderConnection;

    public FollowerService(SetupOptions options, Clock clock) {
        super(options, clock);
        this.leaderHost = options.getReplicaof();
        this.leaderPort = options.getReplicaofPort();
    }

    // Starts the service and initiates connection to the leader
    @Override
    public void start() throws IOException {
        super.start();
        connectToLeader();
    }

    // Establishes socket connection to leader and starts handshake
    private void connectToLeader() throws IOException {
        leaderClientSocket = new Socket(leaderHost, leaderPort);
        leaderClientSocket.setReuseAddress(true);
        leaderConnection = new ConnectionToLeader(this);
        leaderConnection.startHandshake();
    }

    // Reconnects to leader with exponential backoff
    public void reconnectToLeader() {
        Thread reconnectThread = new Thread(() -> {
            int delay = INITIAL_RECONNECT_DELAY_MS;
            while (!isShutdownRequested()) {
                try {
                    log.info("Attempting to reconnect to leader in {}ms...", delay);
                    Thread.sleep(delay);
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

    // Closes connection to leader on shutdown
    @Override
    public void shutdown() {
        super.shutdown();
        if (leaderConnection != null) {
            leaderConnection.terminate();
        }
    }

    // Executes command locally - followers don't propagate to other servers
    @Override
    public void execute(Command command, ClientConnection conn) throws IOException {
        if (hasActiveTransaction(conn) && isQueueableCommand(command)) {
            queueCommand(command);
            conn.sendResponse(new RespSimpleStringValue("QUEUED").asResponse());
            return;
        }

        byte[] response = command.execute(this);
        if (response != null) {
            conn.sendResponse(response);
        }
    }

    // Handles REPLCONF GETACK - responds with bytes received since handshake completed
    @Override
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

    // Followers don't have replicas, so WAIT always returns 0
    @Override
    public int waitForReplicationServers(int numReplicas, long timeoutMillis) {
        return 0;
    }

    // No additional replication info for followers
    @Override
    public void getReplicationInfo(StringBuilder sb) {
        // Followers don't report replication offset info
    }
}
