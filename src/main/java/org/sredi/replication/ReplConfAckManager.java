package org.sredi.replication;

import java.io.IOException;
import java.time.Clock;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sredi.commands.Command;
import org.sredi.commands.ReplConfCommand;

/**
 * Singleton that coordinates REPLCONF ACK requests and responses for the WAIT command.
 * <p>
 * When a client issues WAIT numreplicas timeout:
 * 1. Leader sends REPLCONF GETACK * to all followers
 * 2. Followers respond with REPLCONF ACK <offset>
 * 3. This manager tracks which followers have acknowledged
 * 4. Returns count of acknowledging followers when enough respond or timeout expires
 * <p>
 * Uses object locks to allow multiple concurrent WAIT commands, each tracking its own set of followers.
 */
public final class ReplConfAckManager {

    private static final Logger log = LoggerFactory.getLogger(ReplConfAckManager.class);

    public static final ReplConfAckManager INSTANCE = new ReplConfAckManager();

    // Maps lock object -> set of followers we're waiting for ACKs from
    private final Map<Object, Set<ClientConnection>> pendingAckSets = new ConcurrentHashMap<>();

    // Maps lock object -> set of followers that have sent ACKs
    private final Map<Object, Set<ClientConnection>> receivedAckSets = new ConcurrentHashMap<>();

    // When true, returns immediately without waiting (for testing)
    @Setter
    private boolean testingDontWaitForAck = true;

    private ReplConfAckManager() {
    }

    // Called when a follower sends REPLCONF ACK - notifies any waiting WAIT commands
    public void notifyGotAckFromFollower(ClientConnection connection) {
        pendingAckSets.forEach((lock, waitingFor) -> {
            if (waitingFor.contains(connection)) {
                synchronized (lock) {
                    receivedAckSets.get(lock).add(connection);
                    lock.notifyAll();
                }
            }
        });
    }

    // Sends REPLCONF GETACK to followers and waits for responses
    public int waitForAcksFromFollowerSet(int requestedCount, Set<ClientConnection> followers,
            Clock clock, long timeoutMillis) {

        Object lock = new Object();
        Set<ClientConnection> receivedAcks = new HashSet<>();

        registerWaitSession(lock, followers, receivedAcks);

        try {
            synchronized (lock) {
                sendGetAckToAll(followers);

                if (testingDontWaitForAck) {
                    return followers.size();
                }

                return waitForAcks(lock, receivedAcks, requestedCount, followers.size(), clock, timeoutMillis);
            }
        } finally {
            unregisterWaitSession(lock);
        }
    }

    // Registers this WAIT session so incoming ACKs can be routed to it
    private void registerWaitSession(Object lock, Set<ClientConnection> followers, Set<ClientConnection> receivedAcks) {
        pendingAckSets.put(lock, followers);
        receivedAckSets.put(lock, receivedAcks);
    }

    // Removes this WAIT session after completion
    private void unregisterWaitSession(Object lock) {
        pendingAckSets.remove(lock);
        receivedAckSets.remove(lock);
    }

    // Sends REPLCONF GETACK * to all followers
    private void sendGetAckToAll(Set<ClientConnection> followers) {
        followers.forEach(this::sendGetAckCommand);
    }

    // Blocks until enough ACKs received or timeout expires
    private int waitForAcks(Object lock, Set<ClientConnection> receivedAcks, int requestedCount,
            int followerCount, Clock clock, long timeoutMillis) {
        int numToWaitFor = Math.min(requestedCount, followerCount);
        long deadline = clock.millis() + timeoutMillis;

        try {
            while (clock.millis() < deadline && receivedAcks.size() < numToWaitFor) {
                long remainingMs = deadline - clock.millis();
                if (remainingMs > 0) {
                    lock.wait(remainingMs);
                }
                log.debug("Received {} of {} requested ACKs", receivedAcks.size(), numToWaitFor);
            }
        } catch (InterruptedException e) {
            log.error("Interrupted while waiting for ACKs", e);
            Thread.currentThread().interrupt();
        }

        return receivedAcks.size();
    }

    // Sends REPLCONF GETACK * to a single follower
    private void sendGetAckCommand(ClientConnection connection) {
        ReplConfCommand getAck = new ReplConfCommand(ReplConfCommand.Option.GETACK, "*");
        String commandStr = new String(getAck.asCommand()).toUpperCase();
        log.debug("Sending GETACK to {}: '{}'", connection, Command.responseLogString(commandStr.getBytes()));

        try {
            connection.writeFlush(commandStr.getBytes());
        } catch (IOException e) {
            log.error("Failed to send GETACK to {}: {}", connection, e.getMessage());
        }
    }

    @Override
    public String toString() {
        return "ReplConfAckManager[pending=" + pendingAckSets.size() + ", received=" + receivedAckSets.size() + "]";
    }
}
