package org.sredi.replication;

import java.io.IOException;
import java.time.Clock;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import lombok.Getter;
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

import java.util.Set;

public class LeaderService extends CentralRepository {
    private final static String EMPTY_RDB_BASE64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

    String replicationId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    @Getter
    long totalReplicationOffset = 0L;
    Map<String, Long> replicationOffsets = new HashMap<>();
    Map<String, ConnectionToFollower> replMap = new ConcurrentHashMap<>();

    private ExecutorService executorService = Executors.newFixedThreadPool(12);

    public LeaderService(SetupOptions options, Clock clock) {
        super(options, clock);
    }

    long getFollowerOffset(String follower) {
        return replicationOffsets.getOrDefault(follower, 0L);
    }

    @Override
    public void execute(Command command, ClientConnection conn) throws IOException {
        // for the leader, return the command response and replicate to the followers
        byte[] response = command.execute(this);
        // Note: first complete replication before sending the response

        // check if it is a new follower
        String connectionString = conn.getConnectionString();
        if (command.getType() == Type.PSYNC && !replMap.containsKey(connectionString)) {
            // This command is the final handshake command.
            // Save the connection as a follower so that future commands will be replicated to it
            replMap.put(connectionString, new ConnectionToFollower(this, conn));
        }
        // replicate to the followers
        if (command.isReplicatedCommand()) {
            Iterator<ConnectionToFollower> iter = replMap.values().iterator();
            while (iter.hasNext()) {
                ConnectionToFollower follower = iter.next();
                ClientConnection clientConnection = follower.getFollowerConnection();
                if (clientConnection.isClosed()) {
                    System.out.printf("Follower connection closed: %s%n", conn);
                    iter.remove();
                    continue;
                }
                if (clientConnection != conn) {
                    follower.setTestingDontWaitForAck(false);
                    ReplConfAckManager.INSTANCE.setTestingDontWaitForAck(false);
                    try {
                        clientConnection.writeFlush(command.asCommand());
                    } catch (IOException e) {
                        System.out.printf(
                                "Follower exception during replication connection: %s, exception: %s%n",
                                conn, e.getMessage());
                    }
                }
            }
        }
        if (response != null && response.length > 0) {
            conn.writeFlush(response);
        }
    }

    @Override
    public void getReplicationInfo(StringBuilder sb) {
        sb.append("master_replid:").append(replicationId).append("\n");
        sb.append("master_repl_offset:").append(totalReplicationOffset).append("\n");
    }

    @Override
    public byte[] replicationConfirm(ClientConnection connection, Map<String, RespValue> optionsMap,
            long startBytesOffset) {
        if (optionsMap.containsKey(ReplConfCommand.GETACK_NAME)) {
            String responseValue = String.valueOf(startBytesOffset);
            return new RespArrayValue(new RespValue[] {
                    new RespBulkString(Type.REPLCONF.name().getBytes()),
                    new RespBulkString("ACK".getBytes()),
                    new RespBulkString(responseValue.getBytes()) }).asResponse();
        } else if (optionsMap.containsKey(ReplConfCommand.ACK_NAME)) {
            ReplConfAckManager.INSTANCE.notifyGotAckFromFollower(connection);
            return null;
        }
        return RespConstants.OK;
    }

    @Override
    public int waitForReplicationServers(int numReplicas, long timeoutMillis) {
        boolean useNewWaitMethod = true;
        if (useNewWaitMethod) {
            Set<ClientConnection> followerSet = replMap.values().stream()
                    .map(ConnectionToFollower::getFollowerConnection).collect(Collectors.toSet());
            return ReplConfAckManager.INSTANCE.waitForAcksFromFollowerSet(numReplicas,
                    followerSet, clock, timeoutMillis);
        } else {
            // Note: the waitExecutor should return all replicated services that acknowledge, even
            // if it
            // is greater than requested number, but there is no point in waiting for more responses
            // than we have replicas, so we use min(numReplicas, replMap.size())
            WaitExecutor waitExecutor = new WaitExecutor(numReplicas, replMap.size(),
                    executorService);
            return waitExecutor.wait(replMap.values(), timeoutMillis);
        }
    }

    @Override
    public byte[] psync(Map<String, RespValue> optionsMap) {
        String response = String.format("FULLRESYNC %s %d", replicationId, totalReplicationOffset);
        return new RespSimpleStringValue(response).asResponse();
    }

    @Override
    public byte[] psyncRdb(Map<String, RespValue> optionsMap) {
        byte[] rdbData = Base64.getDecoder().decode(EMPTY_RDB_BASE64);
        return new RespBulkString(rdbData).asResponse(false);
    }
}
