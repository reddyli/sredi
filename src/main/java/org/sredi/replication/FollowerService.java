package org.sredi.replication;

import java.io.IOException;
import java.net.Socket;
import java.time.Clock;
import java.util.List;
import java.util.Map;

import org.sredi.commands.Command;
import org.sredi.commands.Command.Type;
import org.sredi.commands.ReplConfCommand;
import org.sredi.resp.RespArrayValue;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespConstants;
import org.sredi.resp.RespSimpleStringValue;
import org.sredi.resp.RespValue;
import org.sredi.setup.SetupOptions;
import org.sredi.storage.CentralRepository;

public class FollowerService extends CentralRepository {
    private ConnectionToLeader leaderConnection;
    private final String leaderHost;
    private final int leaderPort;
    private Socket leaderClientSocket;

    public FollowerService(SetupOptions options, Clock clock) {
        super(options, clock);

        leaderHost = options.getReplicaof();
        leaderPort = options.getReplicaofPort();
    }

    @Override
    public void getReplicationInfo(StringBuilder sb) {
        // nothing to add for now
    }

    @Override
    public void start() throws IOException {
        super.start();

        leaderClientSocket = new Socket(leaderHost, leaderPort);
        leaderClientSocket.setReuseAddress(true);
        leaderConnection = new ConnectionToLeader(this);

        // initiate the handshake with the leader service
        leaderConnection.startHandshake();
    }

    @Override
    public void shutdown() {
        super.shutdown();
        if (leaderConnection != null) {
            leaderConnection.terminate();
        }
    }

    /**
     * @return the leaderConnection
     */
    public ConnectionToLeader getLeaderConnection() {
        return leaderConnection;
    }

    /**
     * @return the leaderHost
     */
    public String getLeaderHost() {
        return leaderHost;
    }

    /**
     * @return the leaderPort
     */
    public int getLeaderPort() {
        return leaderPort;
    }

    /**
     * @return the leaderClientSocket
     */
    public Socket getLeaderClientSocket() {
        return leaderClientSocket;
    }

    @Override
    public void execute(Command command, ClientConnection conn) throws IOException {
        // Check if we're in a transaction
        List<Command> queue = transactionQueues.get(conn);
        if (queue != null && command.getType() != Type.MULTI && command.getType() != Type.EXEC && command.getType() != Type.DISCARD) {
            // Queue the command instead of executing it
            queueCommand(command);
            conn.sendResponse(new RespSimpleStringValue("QUEUED").asResponse());
            return;
        }

        // Execute the command
        byte[] response = command.execute(this);
        if (response != null) {
            conn.sendResponse(response);
        }
    }

    @Override
    public byte[] replicationConfirm(ClientConnection connection, Map<String, RespValue> optionsMap,
            long startBytesOffset) {
        if (optionsMap.containsKey(ReplConfCommand.GETACK_NAME)) {
            String responseValue = String
                    .valueOf(startBytesOffset - leaderConnection.getHandshakeBytesReceived());
            return new RespArrayValue(new RespValue[] {
                    new RespBulkString(Command.Type.REPLCONF.name().getBytes()),
                    new RespBulkString("ACK".getBytes()),
                    new RespBulkString(responseValue.getBytes()) }).asResponse();
        }
        return RespConstants.OK;
    }

    @Override
    public int waitForReplicationServers(int numReplicas, long timeoutMillis) {
        return 0;
    }
}
