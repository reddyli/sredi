package org.sredi.election;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Spins up three full mesh + election + heartbeat stacks in-process on
 * different ports, with a stub {@link RoleSwitcher} that just records
 * what would have happened. Verifies that the highest-id node is elected
 * leader and the other two become followers of it.
 *
 * Uses real sockets on localhost; mesh ports are client-port + 10000 so we
 * pick unusual high ports to minimise conflicts with anything else running.
 */
class ElectionIntegrationTest {

    private static final String CLUSTER_SPEC =
            "n1@127.0.0.1:45700,n2@127.0.0.1:45701,n3@127.0.0.1:45702";
    private static final long CONVERGENCE_TIMEOUT_MS = 10_000;

    private final List<ClusterMesh> meshes = new ArrayList<>();
    private final List<ElectionService> elections = new ArrayList<>();
    private final List<HeartbeatService> heartbeats = new ArrayList<>();
    private final List<RecordingRoleSwitcher> roleSwitchers = new ArrayList<>();

    @AfterEach
    void teardown() {
        heartbeats.forEach(HeartbeatService::stop);
        elections.forEach(ElectionService::stop);
        meshes.forEach(ClusterMesh::stop);
    }

    @Test
    void highestIdNodeIsElectedLeader() throws Exception {
        startNode("n1");
        startNode("n2");
        startNode("n3");

        // The cluster needs time to: open all 6 outbound peer links, run
        // the bully exchange, broadcast COORDINATOR, and have n1/n2 react.
        awaitUntil(() -> "n3".equals(elections.get(0).currentLeaderId())
                      && "n3".equals(elections.get(1).currentLeaderId())
                      && "n3".equals(elections.get(2).currentLeaderId()),
                CONVERGENCE_TIMEOUT_MS);

        assertEquals("n3", elections.get(0).currentLeaderId());
        assertEquals("n3", elections.get(1).currentLeaderId());
        assertEquals("n3", elections.get(2).currentLeaderId());

        // Only n3 thinks it's the leader.
        assertEquals(ElectionService.State.FOLLOWER, elections.get(0).state());
        assertEquals(ElectionService.State.FOLLOWER, elections.get(1).state());
        assertEquals(ElectionService.State.LEADER,   elections.get(2).state());

        // n1 and n2 were told to follow n3's client port.
        assertEquals(45702, roleSwitchers.get(0).followingPort.get());
        assertEquals(45702, roleSwitchers.get(1).followingPort.get());

        // n3 was told it became leader at least once.
        assertTrue(roleSwitchers.get(2).leaderCalls.get() >= 1,
                "expected becomeLeader() on n3");

        // Epoch converged on the same value across the cluster.
        long e0 = elections.get(0).currentEpoch();
        assertEquals(e0, elections.get(1).currentEpoch());
        assertEquals(e0, elections.get(2).currentEpoch());
        assertTrue(e0 >= 1, "expected epoch >= 1, got " + e0);
    }

    private void startNode(String nodeId) throws Exception {
        ClusterConfig cfg = ClusterConfig.parse(nodeId, CLUSTER_SPEC);
        RecordingRoleSwitcher t = new RecordingRoleSwitcher();
        ClusterMesh mesh = new ClusterMesh(cfg);
        ElectionService election = new ElectionService(mesh, t);
        HeartbeatService heartbeat = new HeartbeatService(mesh, election);
        mesh.addListener(election);
        mesh.addListener(heartbeat);
        mesh.start();
        election.start();
        heartbeat.start();

        meshes.add(mesh);
        elections.add(election);
        heartbeats.add(heartbeat);
        roleSwitchers.add(t);
    }

    /** Polls the condition every 50ms until true or timeout. */
    private static void awaitUntil(BooleanSupplier cond, long timeoutMs)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (System.nanoTime() < deadline) {
            if (cond.getAsBoolean()) return;
            Thread.sleep(50);
        }
        throw new AssertionError("condition not met within " + timeoutMs + "ms");
    }

    /** Captures role transitions without doing any real replication. */
    static final class RecordingRoleSwitcher implements RoleSwitcher {
        final AtomicInteger leaderCalls = new AtomicInteger();
        final AtomicReference<String> followingHost = new AtomicReference<>();
        final AtomicInteger followingPort = new AtomicInteger();

        @Override
        public void becomeLeader() {
            leaderCalls.incrementAndGet();
        }

        @Override
        public void becomeFollowerOf(String host, int port) {
            followingHost.set(host);
            followingPort.set(port);
        }
    }
}
