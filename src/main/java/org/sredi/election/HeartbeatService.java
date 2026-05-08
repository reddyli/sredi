package org.sredi.election;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodic heartbeats and failure detection on top of the mesh.
 *
 *   While LEADER:   broadcast HEARTBEAT every HEARTBEAT_INTERVAL_MS.
 *   While FOLLOWER: if no HEARTBEAT seen for FAILURE_TIMEOUT_MS,
 *                   ask the {@link ElectionService} to start an election.
 *
 * One ticker thread, fired every HEARTBEAT_INTERVAL_MS, decides what
 * to do based on the current role from {@link ElectionService}. This
 * keeps the leadership transition cost-free: a node that was leading
 * a tick ago and is following now will simply skip the broadcast on
 * the next tick.
 */
public final class HeartbeatService implements MeshMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(HeartbeatService.class);

    public static final long HEARTBEAT_INTERVAL_MS = 500;
    public static final long FAILURE_TIMEOUT_MS = 2_000;

    private final ClusterMesh mesh;
    private final ClusterConfig config;
    private final ElectionService election;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "heartbeat");
                t.setDaemon(true);
                return t;
            });

    private volatile long lastHeartbeatNanos = System.nanoTime();

    public HeartbeatService(ClusterMesh mesh, ElectionService election) {
        this.mesh = mesh;
        this.config = mesh.config();
        this.election = election;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::tick,
                HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    public void stop() { scheduler.shutdownNow(); }

    @Override
    public void onMessage(NodeId from, MeshMessage msg) {
        if (msg instanceof MeshMessage.Heartbeat) {
            lastHeartbeatNanos = System.nanoTime();
        }
    }

    private void tick() {
        try {
            if (election.isLeader()) {
                mesh.broadcast(new MeshMessage.Heartbeat(
                        config.self().id(), election.currentEpoch()));
                return;
            }
            long sinceMs = (System.nanoTime() - lastHeartbeatNanos) / 1_000_000L;
            if (sinceMs > FAILURE_TIMEOUT_MS) {
                log.warn("heartbeat: no leader heartbeat for {}ms; starting election", sinceMs);
                lastHeartbeatNanos = System.nanoTime(); // throttle re-trigger
                election.startElection();
            }
        } catch (Exception e) {
            log.error("heartbeat tick failed", e);
        }
    }
}
