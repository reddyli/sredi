package org.sredi.election;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Accepts INBOUND mesh connections from other nodes.
 *
 * Each accepted socket is read on its own daemon thread. The first line
 * MUST be HELLO so we can identify the peer; subsequent lines are
 * dispatched to the handler with the resolved {@link NodeId}.
 *
 * Note: the inbound socket is read-only from our side. Replies (e.g. OK
 * to ELECTION) flow over the corresponding {@link OutboundPeerLink},
 * which keeps the routing logic uniform regardless of which side dialled.
 */
public final class InboundMeshAcceptor {
    private static final Logger log = LoggerFactory.getLogger(InboundMeshAcceptor.class);

    private final ClusterConfig config;
    private final MeshMessageHandler handler;
    private ServerSocket serverSocket;
    private Thread acceptThread;
    private volatile boolean stopped = false;

    public InboundMeshAcceptor(ClusterConfig config, MeshMessageHandler handler) {
        this.config = config;
        this.handler = handler;
    }

    public void start() throws IOException {
        int port = config.self().meshPort();
        serverSocket = new ServerSocket(port);
        serverSocket.setReuseAddress(true);
        log.info("mesh: listening on {}", port);

        acceptThread = new Thread(this::acceptLoop, "mesh-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    public void stop() {
        stopped = true;
        if (serverSocket != null) {
            try { serverSocket.close(); } catch (IOException ignored) { }
        }
        if (acceptThread != null) acceptThread.interrupt();
    }

    private void acceptLoop() {
        while (!stopped) {
            try {
                Socket s = serverSocket.accept();
                s.setTcpNoDelay(true);
                s.setKeepAlive(true);
                Thread t = new Thread(() -> handleInbound(s), "mesh-in");
                t.setDaemon(true);
                t.start();
            } catch (IOException e) {
                if (!stopped) log.debug("mesh accept error: {}", e.getMessage());
            }
        }
    }

    private void handleInbound(Socket s) {
        NodeId from = null;
        try (BufferedReader r = new BufferedReader(
                new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8))) {

            String first = r.readLine();
            MeshMessage hello = MeshMessage.parse(first);
            if (!(hello instanceof MeshMessage.Hello h)) {
                log.warn("mesh: dropped inbound, expected HELLO got '{}'", first);
                return;
            }
            from = config.byId(h.senderId());
            if (from == null) {
                log.warn("mesh: dropped inbound from unknown id '{}'", h.senderId());
                return;
            }
            log.info("mesh: accepted inbound from {}", from);

            String line;
            while (!stopped && (line = r.readLine()) != null) {
                MeshMessage msg = MeshMessage.parse(line);
                if (msg == null) continue;
                try {
                    handler.onMessage(from, msg);
                } catch (Exception ex) {
                    log.error("mesh handler threw on {} from {}", msg, from.id(), ex);
                }
            }
        } catch (IOException e) {
            if (!stopped) {
                log.debug("mesh inbound from {} closed: {}",
                        from == null ? "?" : from.id(), e.getMessage());
            }
        } finally {
            try { s.close(); } catch (IOException ignored) { }
        }
    }
}
