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

public class ConnectionManager {
    private static final Logger log = LoggerFactory.getLogger(ConnectionManager.class);
    private final Deque<ClientConnection> clientSockets = new ConcurrentLinkedDeque<>();
    private final Map<ClientConnection, Queue<RespValue>> clientValues = new ConcurrentHashMap<>();

    public void start(ExecutorService executorService) throws IOException {
        executorService.submit(() -> {
            for (;;) {
                boolean didRead = false;
                Iterator<ClientConnection> iter = clientSockets.iterator();
                for (; iter.hasNext();) {
                    ClientConnection conn = iter.next();
                    if (conn.isClosed()) {
                        log.debug("Connection closed by the server: {}", conn);
                        clientValues.remove(conn);
                        iter.remove();
                    }
                    while (conn.available() > 0) {
                        didRead = true;
                        log.trace("ConnectionManager: about to read from connection, available: {} {}", conn.available(), conn);
                        RespValue value = null;
                        try {
                            value = conn.readValue();
                        } catch (Exception e) {
                            log.error("ConnectionManager read exception conn: {} {} \"{}\"", conn, e.getClass().getSimpleName(), e.getMessage());
                        }
                        if (value != null) {
                            getClientValuesQueue(conn).offer(value);
                            conn.notifyNewValueAvailable();
                        }
                    }
                }
                // if there was nothing to be read, then sleep a little
                if (!didRead) {
                    Thread.sleep(80L);
                }
            }
        });
    }

    private Queue<RespValue> getClientValuesQueue(ClientConnection conn) {
        return clientValues.computeIfAbsent(conn, (key) -> new ConcurrentLinkedQueue<RespValue>());
    }

    public void addConnection(ClientConnection conn) {
        clientSockets.addLast(conn);
    }

    public void addPriorityConnection(ClientConnection priorityConnection) {
        clientSockets.addFirst(priorityConnection);
    }

    public void closeAllConnections() {
        for (ClientConnection conn : clientSockets) {
            try {
                log.debug("Closing connection to client: {}, opened: {}", conn, !conn.isClosed());
                if (!conn.isClosed()) {
                    conn.close();
                }
            } catch (IOException e) {
                log.error("IOException: {}", e.getMessage());
            }
        }
    }

    public int getNumConnections() {
        return clientSockets.size();
    }

    public boolean getNextValue(BiConsumer<ClientConnection, RespValue> valueHandler) {
        Iterator<ClientConnection> iter = clientSockets.iterator();
        boolean foundValue = false;
        for (; !foundValue && iter.hasNext();) {
            ClientConnection conn = iter.next();
            Queue<RespValue> valuesQueue = getClientValuesQueue(conn);
            try {
                if (!valuesQueue.isEmpty()) {
                    RespValue value = valuesQueue.poll();
                    valueHandler.accept(conn, value);
                    foundValue = true;
                }
            } catch (Exception e) {
                log.error("ConnectionManager nextValue exception conn: {} {} \"{}\"", conn, e.getClass().getSimpleName(), e.getMessage());
            }
        }
        return foundValue;
    }

    public RespValue getNextValue(ClientConnection conn) {
        if (!clientValues.get(conn).isEmpty()) {
            return clientValues.get(conn).poll();
        } else {
            return null;
        }
    }

}
