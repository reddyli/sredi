package org.sredi.election;

import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bully algorithm state machine.
 *
 * Three states:
 *   FOLLOWER  - we believe someone else is leader (or no one yet).
 *   ELECTING  - we sent ELECTION to higher-id peers and are waiting.
 *   LEADER    - we won and broadcast COORDINATOR.
 *
 * Triggers: any node may start an election (at boot, on heartbeat
 * timeout, or on receiving ELECTION from a lower-id node). The
 * highest-id node always wins because every lower-id node hears at
 * least one OK and stops electing.
 */
public final class ElectionService implements MeshMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(ElectionService.class);

    private static final long ELECTION_TIMEOUT_MS = 1_500;
    private static final long COORDINATOR_WAIT_MS = 3_000;

    public enum State { FOLLOWER, ELECTING, LEADER }

    private final ClusterMesh mesh;
    private final ClusterConfig config;
    private final RoleSwitcher roleSwitcher;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "election");
                t.setDaemon(true);
                return t;
            });
    private final Random random = new Random();
    private final Object lock = new Object();

    private State state = State.FOLLOWER;
    private long currentEpoch = 0;
    private String currentLeaderId;
    private boolean okReceived;
    private ScheduledFuture<?> electionTimer;
    private ScheduledFuture<?> waitTimer;

    public ElectionService(ClusterMesh mesh, RoleSwitcher roleSwitcher) {
        this.mesh = mesh;
        this.config = mesh.config();
        this.roleSwitcher = roleSwitcher;
    }

    public void start() {
        // Stagger startup so peers don't all elect at the same instant.
        long delayMs = 300 + random.nextInt(700);
        scheduler.schedule(this::startElection, delayMs, TimeUnit.MILLISECONDS);
    }

    public void stop() { scheduler.shutdownNow(); }

    public State state() { synchronized (lock) { return state; } }
    public boolean isLeader() { synchronized (lock) { return state == State.LEADER; } }
    public long currentEpoch() { synchronized (lock) { return currentEpoch; } }
    public String currentLeaderId() { synchronized (lock) { return currentLeaderId; } }

    public void startElection() {
        synchronized (lock) {
            if (state == State.ELECTING) return;
            state = State.ELECTING;
            okReceived = false;
            log.info("election: starting at epoch {}", currentEpoch);

            List<NodeId> higher = config.higherPeers();
            if (higher.isEmpty()) { winElection(); return; }

            for (NodeId p : higher) {
                mesh.sendTo(p.id(), new MeshMessage.Election(config.self().id(), currentEpoch));
            }
            cancel(electionTimer);
            electionTimer = scheduler.schedule(this::onElectionTimeout,
                    ELECTION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }
    }

    private void onElectionTimeout() {
        synchronized (lock) {
            if (state != State.ELECTING) return;
            if (okReceived) {
                cancel(waitTimer);
                waitTimer = scheduler.schedule(this::startElection,
                        COORDINATOR_WAIT_MS, TimeUnit.MILLISECONDS);
            } else {
                winElection();
            }
        }
    }

    /** Must be called with {@link #lock} held. */
    private void winElection() {
        currentEpoch += 1;
        currentLeaderId = config.self().id();
        state = State.LEADER;
        log.info("election: won at epoch {}", currentEpoch);
        mesh.broadcast(new MeshMessage.Coordinator(config.self().id(), currentEpoch));
        roleSwitcher.becomeLeader();
    }

    @Override
    public void onMessage(NodeId from, MeshMessage msg) {
        switch (msg) {
            case MeshMessage.Election e -> handleElection(from, e);
            case MeshMessage.Ok o -> handleOk(from, o);
            case MeshMessage.Coordinator c -> handleCoordinator(c);
            case MeshMessage.Heartbeat h -> handleHeartbeat(from, h);
            case MeshMessage.Hello ignored -> { /* identity already resolved */ }
        }
    }

    private void handleElection(NodeId from, MeshMessage.Election e) {
        // Reply OK and (asynchronously) start our own election: we are
        // higher than the sender, so we should become leader before they do.
        mesh.sendTo(from.id(), new MeshMessage.Ok(config.self().id(), currentEpoch));
        scheduler.execute(this::startElection);
    }

    private void handleOk(NodeId from, MeshMessage.Ok o) {
        synchronized (lock) {
            if (state == State.ELECTING) {
                okReceived = true;
                log.debug("election: OK from {}", from.id());
            }
        }
    }

    private void handleCoordinator(MeshMessage.Coordinator c) {
        synchronized (lock) {
            if (c.epoch() < currentEpoch) return; // stale announcement
            currentEpoch = c.epoch();
            currentLeaderId = c.leaderId();
            cancel(electionTimer);
            cancel(waitTimer);
            applyLeaderChange(c.leaderId());
        }
    }

    private void handleHeartbeat(NodeId from, MeshMessage.Heartbeat h) {
        synchronized (lock) {
            if (h.epoch() < currentEpoch) return;
            if (h.epoch() > currentEpoch || !from.id().equals(currentLeaderId)) {
                currentEpoch = h.epoch();
                currentLeaderId = h.leaderId();
                applyLeaderChange(h.leaderId());
            }
        }
    }

    /** Must be called with {@link #lock} held. */
    private void applyLeaderChange(String leaderId) {
        if (leaderId.equals(config.self().id())) {
            state = State.LEADER;
            return;
        }
        state = State.FOLLOWER;
        NodeId leader = config.byId(leaderId);
        if (leader == null) return;
        try {
            roleSwitcher.becomeFollowerOf(leader.host(), leader.port());
        } catch (Exception e) {
            log.error("becomeFollowerOf({}) failed", leader, e);
        }
    }

    private static void cancel(ScheduledFuture<?> f) {
        if (f != null) f.cancel(false);
    }
}
