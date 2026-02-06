package org.sredi.replication;

import java.io.IOException;
import java.net.Socket;
import java.time.Clock;
import java.util.Map;

import lombok.Getter;
import org.sredi.commands.Command;
import org.sredi.commands.ReplConfCommand;
import org.sredi.resp.RespArrayValue;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespConstants;
import org.sredi.resp.RespSimpleStringValue;
import org.sredi.resp.RespValue;
import org.sredi.setup.SetupOptions;
import org.sredi.storage.CentralRepository;

/**
 * Follower (replica) implementation of CentralRepository.
 * Connects to a leader server, performs handshake, and receives replicated commands.
 * Does not propagate commands further - it's a read replica that stays in sync with the leader.
 */
public class FollowerService extends CentralRepository {

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
