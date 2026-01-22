package org.sredi.replication;

import java.io.IOException;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sredi.resp.RespValue;

/**
 * Manages all active client connections and their incoming value queues.
 * Runs a background reader thread that polls connections for data and queues parsed values.
 */
public class ConnectionManager {
    private static final Logger log = LoggerFactory.getLogger(ConnectionManager.class);
    private static final long IDLE_POLL_INTERVAL_MS = 80L;

    // All active connections, priority connections at front
    private final Deque<ClientConnection> connections = new ConcurrentLinkedDeque<>();

    // Per-connection queue of parsed RESP values waiting to be processed
    private final Map<ClientConnection, Queue<RespValue>> pendingValues = new ConcurrentHashMap<>();

    // Starts the background thread that reads from all connections
    public void start(ExecutorService executorService) {
        executorService.submit(this::runReaderLoop);
    }

    // Continuously polls all connections for incoming data
    private void runReaderLoop() {
        while (true) {
            boolean didRead = pollAllConnections();
            if (!didRead) {
                sleepQuietly(IDLE_POLL_INTERVAL_MS);
            }
        }
    }

    // Polls each connection, reads available data, returns true if any data was read
    private boolean pollAllConnections() {
        boolean didRead = false;
        Iterator<ClientConnection> iter = connections.iterator();

        while (iter.hasNext()) {
            ClientConnection conn = iter.next();

            if (conn.isClosed()) {
                log.debug("Removing closed connection: {}", conn);
                pendingValues.remove(conn);
                iter.remove();
                continue;
            }

            didRead |= readAvailableValues(conn);
        }
        return didRead;
    }

    // Reads all available values from a connection and queues them
    private boolean readAvailableValues(ClientConnection conn) {
        boolean didRead = false;
        try {
            while (conn.available() > 0) {
                didRead = true;
                log.trace("Reading from connection, available bytes: {} {}", conn.available(), conn);

                RespValue value = conn.readValue();
                if (value != null) {
                    getOrCreateQueue(conn).offer(value);
                    conn.notifyNewValueAvailable();
                }
            }
        } catch (Exception e) {
            log.error("Error reading from {}: {} \"{}\"", conn, e.getClass().getSimpleName(), e.getMessage());
        }
        return didRead;
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // Gets or creates the value queue for a connection
    private Queue<RespValue> getOrCreateQueue(ClientConnection conn) {
        return pendingValues.computeIfAbsent(conn, k -> new ConcurrentLinkedQueue<>());
    }

    // Adds a new connection to be managed
    public void addConnection(ClientConnection conn) {
        connections.addLast(conn);
    }

    // Adds a connection at the front (e.g., leader connection for followers)
    public void addPriorityConnection(ClientConnection conn) {
        connections.addFirst(conn);
    }

    // Closes all managed connections
    public void closeAllConnections() {
        for (ClientConnection conn : connections) {
            try {
                if (!conn.isClosed()) {
                    log.debug("Closing connection: {}", conn);
                    conn.close();
                }
            } catch (IOException e) {
                log.error("Error closing connection: {}", e.getMessage());
            }
        }
    }

    public int getNumConnections() {
        return connections.size();
    }

    // Finds the next available value across all connections and passes it to the handler
    public boolean getNextValue(BiConsumer<ClientConnection, RespValue> valueHandler) {
        for (ClientConnection conn : connections) {
            Queue<RespValue> queue = getOrCreateQueue(conn);
            if (!queue.isEmpty()) {
                try {
                    RespValue value = queue.poll();
                    valueHandler.accept(conn, value);
                    return true;
                } catch (Exception e) {
                    log.error("Error handling value from {}: {} \"{}\"",
                            conn, e.getClass().getSimpleName(), e.getMessage());
                }
            }
        }
        return false;
    }

    // Gets the next value for a specific connection, or null if none available
    public RespValue getNextValue(ClientConnection conn) {
        Queue<RespValue> queue = pendingValues.get(conn);
        return (queue != null && !queue.isEmpty()) ? queue.poll() : null;
    }
}
