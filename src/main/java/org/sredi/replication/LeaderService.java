package org.sredi.replication;

import java.io.IOException;
import java.time.Clock;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
import org.sredi.storage.Orchestrator;

/**
 * Leader (master) implementation of Orchestrator.
 * Handles command execution and propagates write commands to all connected followers.
 * Manages replication state and responds to PSYNC requests from new followers.
 */
public class LeaderService extends Orchestrator {

    // Empty RDB snapshot sent to followers during FULLRESYNC (base64 encoded)
    private static final String EMPTY_RDB_BASE64 =
            "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

    // Unique identifier for this replication stream
    private final String replicationId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

    // Total bytes of commands sent to followers (used for partial resync)
    @Getter
    private long totalReplicationOffset = 0L;

    // Maps follower ID to its connection wrapper
    private final Map<String, ConnectionToFollower> followers = new ConcurrentHashMap<>();

    public LeaderService(SetupOptions options, Clock clock) {
        super(options, clock);
    }

    // Executes a command, sends response to client, and replicates to followers if needed
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

        replicateToFollowers(command);
    }

    // Sends write commands to all connected followers
    private void replicateToFollowers(Command command) throws IOException {
        if (command.isReplicatedCommand()) {
            for (ConnectionToFollower follower : followers.values()) {
                follower.sendCommand(command);
            }
        }
    }

    // Appends replication info for INFO command
    @Override
    public void getReplicationInfo(StringBuilder sb) {
        sb.append("master_replid:").append(replicationId).append("\n");
        sb.append("master_repl_offset:").append(totalReplicationOffset).append("\n");
    }

    // Handles REPLCONF command - either responds to GETACK or processes ACK from follower
    @Override
    public byte[] replicationConfirm(ClientConnection connection, Map<String, RespValue> optionsMap,
            long startBytesOffset) {
        if (optionsMap.containsKey(ReplConfCommand.GETACK_NAME)) {
            return buildReplConfAckResponse(startBytesOffset);
        } else if (optionsMap.containsKey(ReplConfCommand.ACK_NAME)) {
            ReplConfAckManager.INSTANCE.notifyGotAckFromFollower(connection);
            return null;
        }
        return RespConstants.OK;
    }

    // Builds REPLCONF ACK response with current byte offset
    private byte[] buildReplConfAckResponse(long bytesOffset) {
        return new RespArrayValue(new RespValue[] {
                new RespBulkString(Type.REPLCONF.name().getBytes()),
                new RespBulkString("ACK".getBytes()),
                new RespBulkString(String.valueOf(bytesOffset).getBytes())
        }).asResponse();
    }

    // Blocks until numReplicas followers acknowledge, or timeout expires
    @Override
    public int waitForReplicationServers(int numReplicas, long timeoutMillis) {
        Set<ClientConnection> followerConnections = followers.values().stream()
                .map(ConnectionToFollower::getFollowerConnection)
                .collect(Collectors.toSet());
        return ReplConfAckManager.INSTANCE.waitForAcksFromFollowerSet(
                numReplicas, followerConnections, clock, timeoutMillis);
    }

    // Responds to PSYNC with FULLRESYNC and replication ID
    @Override
    public byte[] psync(Map<String, RespValue> optionsMap) {
        String response = String.format("FULLRESYNC %s %d", replicationId, totalReplicationOffset);
        return new RespSimpleStringValue(response).asResponse();
    }

    // Sends empty RDB snapshot to follower during full resync
    @Override
    public byte[] psyncRdb(Map<String, RespValue> optionsMap) {
        byte[] rdbData = Base64.getDecoder().decode(EMPTY_RDB_BASE64);
        return new RespBulkString(rdbData).asResponse(false);
    }
}
