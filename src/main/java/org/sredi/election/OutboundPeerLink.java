package org.sredi.election;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Persistent OUTBOUND TCP connection to one peer's mesh port.
 *
 * Owns a single background thread that:
 *   1. dials peer.host:peer.meshPort with exponential backoff,
 *   2. sends HELLO once connected,
 *   3. reads inbound lines and dispatches to the handler,
 *   4. reconnects on any failure until {@link #stop()} is called.
 *
 * Outbound writes are synchronized on the writer so multiple senders
 * (heartbeat thread, election thread, etc.) can share a link safely.
 */
public final class OutboundPeerLink {
    private static final Logger log = LoggerFactory.getLogger(OutboundPeerLink.class);

    private static final int CONNECT_TIMEOUT_MS = 2_000;
    private static final int INITIAL_BACKOFF_MS = 500;
    private static final int MAX_BACKOFF_MS = 5_000;

    private final NodeId self;
    private final NodeId peer;
    private final MeshMessageHandler handler;

    private volatile Thread worker;
    private volatile Socket socket;
    private volatile PrintWriter writer;
    private volatile boolean stopped = false;

    public OutboundPeerLink(NodeId self, NodeId peer, MeshMessageHandler handler) {
        this.self = self;
        this.peer = peer;
        this.handler = handler;
    }

    public NodeId peer() { return peer; }

    public void start() {
        if (worker != null) return;
        worker = new Thread(this::runLoop, "mesh-out-" + peer.id());
        worker.setDaemon(true);
        worker.start();
    }

    public void stop() {
        stopped = true;
        closeQuietly();
        if (worker != null) worker.interrupt();
    }

    /**
     * Best-effort send. Returns true if the bytes were handed to the kernel
     * buffer, false if no link is currently up. Never throws.
     */
    public boolean send(MeshMessage msg) {
        PrintWriter w = writer;
        if (w == null) return false;
        synchronized (w) {
            w.print(msg.encode());
            w.print('\n');
            w.flush();
            return !w.checkError();
        }
    }

    private void runLoop() {
        int backoff = INITIAL_BACKOFF_MS;
        while (!stopped) {
            try {
                connectAndRead();
                backoff = INITIAL_BACKOFF_MS;
            } catch (IOException e) {
                if (!stopped) {
                    log.debug("peer {} link error: {}", peer.id(), e.getMessage());
                }
            } finally {
                closeQuietly();
            }
            if (stopped) break;
            try {
                Thread.sleep(backoff);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
            backoff = Math.min(backoff * 2, MAX_BACKOFF_MS);
        }
    }

    private void connectAndRead() throws IOException {
        Socket s = new Socket();
        s.connect(new InetSocketAddress(peer.host(), peer.meshPort()), CONNECT_TIMEOUT_MS);
        s.setTcpNoDelay(true);
        s.setKeepAlive(true);
        this.socket = s;
        this.writer = new PrintWriter(
                new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), false);
        log.info("mesh: connected outbound to {}", peer);

        // First message on any link is HELLO so the receiver can identify us.
        send(new MeshMessage.Hello(self.id()));

        BufferedReader r = new BufferedReader(
                new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
        String line;
        while (!stopped && (line = r.readLine()) != null) {
            MeshMessage msg = MeshMessage.parse(line);
            if (msg == null) {
                log.debug("mesh: dropped malformed line from {}: {}", peer.id(), line);
                continue;
            }
            try {
                handler.onMessage(peer, msg);
            } catch (Exception ex) {
                log.error("mesh handler threw on {} from {}", msg, peer.id(), ex);
            }
        }
    }

    private void closeQuietly() {
        Socket s = socket;
        socket = null;
        writer = null;
        if (s != null) {
            try { s.close(); } catch (IOException ignored) { }
        }
    }
}
