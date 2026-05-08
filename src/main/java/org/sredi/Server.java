package org.sredi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sredi.election.ClusterConfig;
import org.sredi.election.ClusterMesh;
import org.sredi.election.ElectionService;
import org.sredi.election.HeartbeatService;
import org.sredi.setup.SetupOptions;
import org.sredi.storage.Orchestrator;

import java.io.IOException;
import java.time.Clock;

public class Server {
    private static final Logger log = LoggerFactory.getLogger(Server.class);

    private final SetupOptions options;

    public Server(String... args) {
        this.options = new SetupOptions();
        if (!options.parseArgs(args)) {
            throw new IllegalArgumentException("Invalid arguments");
        }
    }

    public void run() {
        Orchestrator orchestrator = Orchestrator.newInstance(options, Clock.systemUTC());
        ClusterMesh mesh = null;
        ElectionService election = null;
        HeartbeatService heartbeat = null;
        try {
            orchestrator.start();

            // Cluster mesh + Bully election are opt-in via --cluster.
            // Without it, the node behaves exactly like before.
            if (options.getCluster() != null) {
                ClusterConfig cfg = ClusterConfig.parse(options.getNodeId(), options.getCluster());
                mesh = new ClusterMesh(cfg);
                election = new ElectionService(mesh, orchestrator);
                heartbeat = new HeartbeatService(mesh, election);
                mesh.addListener(election);
                mesh.addListener(heartbeat);
                mesh.start();
                election.start();
                heartbeat.start();
            }

            orchestrator.runCommandLoop();
            log.info("Event loop terminated");
        } catch (IOException e) {
            log.error("IOException: {}", e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("InterruptedException: {}", e.getMessage());
        } finally {
            if (heartbeat != null) heartbeat.stop();
            if (election != null) election.stop();
            if (mesh != null) mesh.stop();
            orchestrator.shutdown();
        }
    }
}
