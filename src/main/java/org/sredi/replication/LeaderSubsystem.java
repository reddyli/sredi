package org.sredi.replication;

import java.io.IOException;
import java.time.Clock;
import java.util.Base64;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sredi.commands.Command;
import org.sredi.commands.Command.Type;
import org.sredi.commands.ReplConfCommand;
import org.sredi.resp.RespArrayValue;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespConstants;
import org.sredi.resp.RespSimpleStringValue;
import org.sredi.resp.RespValue;

/**
 * Pluggable subsystem encapsulating leader-only replication state and behavior.
 * Owns the connected followers, replication offset, and replication id.
 * Designed to be started and stopped at runtime so a node can transition
 * into and out of the leader role without restarting the JVM.
 */
public class LeaderSubsystem {
    private static final Logger log = LoggerFactory.getLogger(LeaderSubsystem.class);

    // Empty RDB snapshot sent to followers during FULLRESYNC (base64 encoded)
    private static final String EMPTY_RDB_BASE64 =
            "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

    @Getter
    private final ConnectionManager connectionManager;
    private final Clock clock;
    private final int maxReplBacklog;

    @Getter
    private final String replicationId;

    @Getter
    private long totalReplicationOffset = 0L;

    private final Map<String, ConnectionToFollower> followers = new ConcurrentHashMap<>();

    private volatile boolean stopped = false;

    public LeaderSubsystem(ConnectionManager connectionManager, Clock clock, int maxReplBacklog) {
        this(connectionManager, clock, maxReplBacklog, "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb");
    }

    public LeaderSubsystem(ConnectionManager connectionManager, Clock clock, int maxReplBacklog,
            String replicationId) {
        this.connectionManager = connectionManager;
        this.clock = clock;
        this.maxReplBacklog = maxReplBacklog;
        this.replicationId = replicationId;
    }

    // No-op start hook; followers register themselves via PSYNC after this is alive
    public void start() {
        log.info("Leader subsystem started, replicationId={}", replicationId);
    }

    // Closes every follower connection and clears the registry
    public void stop() {
        if (stopped) return;
        stopped = true;
        for (Map.Entry<String, ConnectionToFollower> entry : followers.entrySet()) {
            try {
                entry.getValue().getFollowerConnection().close();
            } catch (IOException e) {
                log.warn("Error closing follower {}: {}", entry.getKey(), e.getMessage());
            }
        }
        followers.clear();
        log.info("Leader subsystem stopped");
    }

    // Registers a client connection as a follower for replication
    public void registerFollower(ClientConnection conn) {
        if (stopped) {
            log.warn("Refusing follower registration: subsystem stopped");
            return;
        }
        String followerId = conn.getConnectionString();
        ConnectionToFollower followerConn = new ConnectionToFollower(this, conn, maxReplBacklog);
        followerConn.startReplicationThread();
        followers.put(followerId, followerConn);
        log.info("Registered follower: {}", followerId);
    }

    // Sends write commands to all connected followers
    public void replicate(Command command) throws IOException {
        if (!command.isReplicatedCommand() || stopped) return;
        Iterator<Map.Entry<String, ConnectionToFollower>> iter = followers.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, ConnectionToFollower> entry = iter.next();
            if (!entry.getValue().sendCommand(command)) {
                log.warn("Disconnecting lagging follower: {}", entry.getKey());
                iter.remove();
            }
        }
    }

    // Handles REPLCONF GETACK or ACK from followers
    public byte[] replicationConfirm(ClientConnection connection, Map<String, RespValue> optionsMap,
            long startBytesOffset) {
        if (optionsMap.containsKey(ReplConfCommand.GETACK_NAME)) {
            return new RespArrayValue(new RespValue[] {
                    new RespBulkString(Type.REPLCONF.name().getBytes()),
                    new RespBulkString("ACK".getBytes()),
                    new RespBulkString(String.valueOf(startBytesOffset).getBytes())
            }).asResponse();
        } else if (optionsMap.containsKey(ReplConfCommand.ACK_NAME)) {
            ReplConfAckManager.INSTANCE.notifyGotAckFromFollower(connection);
            return null;
        }
        return RespConstants.OK;
    }

    // Blocks until numReplicas followers acknowledge, or timeout expires
    public int waitForReplicas(int numReplicas, long timeoutMillis) {
        Set<ClientConnection> followerConnections = followers.values().stream()
                .map(ConnectionToFollower::getFollowerConnection)
                .collect(Collectors.toSet());
        return ReplConfAckManager.INSTANCE.waitForAcksFromFollowerSet(
                numReplicas, followerConnections, clock, timeoutMillis);
    }

    // Responds to PSYNC with FULLRESYNC and replication ID
    public byte[] psyncResponse() {
        String response = String.format("FULLRESYNC %s %d", replicationId, totalReplicationOffset);
        return new RespSimpleStringValue(response).asResponse();
    }

    // Sends empty RDB snapshot to follower during full resync
    public byte[] psyncRdb() {
        byte[] rdbData = Base64.getDecoder().decode(EMPTY_RDB_BASE64);
        return new RespBulkString(rdbData).asResponse(false);
    }

    // Appends replication info for INFO command
    public void appendReplicationInfo(StringBuilder sb) {
        sb.append("master_replid:").append(replicationId).append("\n");
        sb.append("master_repl_offset:").append(totalReplicationOffset).append("\n");
    }
}
