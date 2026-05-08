package org.sredi.election;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Top-level cluster mesh: one {@link OutboundPeerLink} per peer plus a
 * single {@link InboundMeshAcceptor} for inbound connections, with a
 * fan-out handler registry.
 *
 * Both directions deliver into the same handler list so handlers don't
 * have to care which socket carried a message.
 *
 * The mesh is intentionally separate from the replication channel:
 *   - replication carries data writes between leader and followers,
 *   - the mesh carries small fixed-size control messages between
 *     every pair of nodes, all the time, regardless of role.
 */
public final class ClusterMesh {
    private static final Logger log = LoggerFactory.getLogger(ClusterMesh.class);

    private final ClusterConfig config;
    private final Map<String, OutboundPeerLink> outbound = new HashMap<>();
    private final InboundMeshAcceptor server;
    private final List<MeshMessageHandler> handlers = new CopyOnWriteArrayList<>();
    private volatile boolean started = false;

    public ClusterMesh(ClusterConfig config) {
        this.config = config;
        MeshMessageHandler fanout = (from, msg) -> {
            for (MeshMessageHandler h : handlers) {
                try { h.onMessage(from, msg); }
                catch (Exception e) {
                    log.error("handler threw on {} from {}", msg, from.id(), e);
                }
            }
        };
        this.server = new InboundMeshAcceptor(config, fanout);
        for (NodeId peer : config.peers()) {
            outbound.put(peer.id(), new OutboundPeerLink(config.self(), peer, fanout));
        }
    }

    public ClusterConfig config() { return config; }

    public void addListener(MeshMessageHandler h) {
        handlers.add(h);
    }

    public void start() throws IOException {
        if (started) return;
        started = true;
        server.start();
        for (OutboundPeerLink link : outbound.values()) link.start();
        log.info("mesh: started with {} peer(s)", outbound.size());
    }

    public void stop() {
        if (!started) return;
        started = false;
        for (OutboundPeerLink link : outbound.values()) link.stop();
        server.stop();
    }

    /**
     * Send to one peer over its outbound link. Returns false if the link
     * is currently down (the caller decides whether that's fatal or fine
     * to retry on the next tick).
     */
    public boolean sendTo(String peerId, MeshMessage msg) {
        OutboundPeerLink link = outbound.get(peerId);
        if (link == null) {
            log.warn("mesh: no outbound link for unknown peer '{}'", peerId);
            return false;
        }
        return link.send(msg);
    }

    /** Fire-and-forget broadcast to every peer; failures are logged at debug. */
    public void broadcast(MeshMessage msg) {
        for (OutboundPeerLink link : outbound.values()) {
            if (!link.send(msg)) {
                log.debug("mesh: broadcast to {} dropped (link down)", link.peer().id());
            }
        }
    }
}
