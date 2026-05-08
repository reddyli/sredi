package org.sredi.election;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Static peer-set configuration parsed from CLI flags.
 *
 *   --node-id n2
 *   --cluster n1@127.0.0.1:6379,n2@127.0.0.1:6380,n3@127.0.0.1:6381
 *
 * The cluster flag must include this node itself (so every node sees the
 * same membership view). Dynamic membership is intentionally out of scope
 * for the first iteration of leader election.
 */
public final class ClusterConfig {

    private final NodeId self;
    private final List<NodeId> allNodes;
    private final Map<String, NodeId> byId;

    private ClusterConfig(NodeId self, List<NodeId> allNodes) {
        this.self = self;
        this.allNodes = Collections.unmodifiableList(allNodes);
        Map<String, NodeId> map = new LinkedHashMap<>();
        for (NodeId n : allNodes) map.put(n.id(), n);
        this.byId = Collections.unmodifiableMap(map);
    }

    public NodeId self() { return self; }
    public List<NodeId> allNodes() { return allNodes; }
    public NodeId byId(String id) { return byId.get(id); }

    /** All nodes other than self, in declaration order. */
    public List<NodeId> peers() {
        List<NodeId> out = new ArrayList<>(allNodes.size() - 1);
        for (NodeId n : allNodes) if (!n.equals(self)) out.add(n);
        return out;
    }

    /** Peers strictly higher than self (used by Bully to send ELECTION). */
    public List<NodeId> higherPeers() {
        List<NodeId> out = new ArrayList<>();
        for (NodeId n : allNodes) {
            if (!n.equals(self) && n.compareTo(self) > 0) out.add(n);
        }
        return out;
    }

    /**
     * Parse a comma-separated cluster spec and locate self by id.
     *
     * spec format: "id1@host1:port1,id2@host2:port2,..."
     */
    public static ClusterConfig parse(String selfId, String spec) {
        Objects.requireNonNull(selfId, "node-id");
        Objects.requireNonNull(spec, "cluster");

        List<NodeId> nodes = new ArrayList<>();
        for (String entry : spec.split(",")) {
            String e = entry.trim();
            if (e.isEmpty()) continue;
            nodes.add(parseEntry(e));
        }
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("cluster spec is empty");
        }

        NodeId self = null;
        for (NodeId n : nodes) {
            if (n.id().equals(selfId)) { self = n; break; }
        }
        if (self == null) {
            throw new IllegalArgumentException(
                    "node-id '" + selfId + "' not present in cluster spec");
        }
        return new ClusterConfig(self, nodes);
    }

    private static NodeId parseEntry(String entry) {
        int at = entry.indexOf('@');
        int colon = entry.lastIndexOf(':');
        if (at <= 0 || colon <= at + 1 || colon == entry.length() - 1) {
            throw new IllegalArgumentException(
                    "bad cluster entry '" + entry + "', expected id@host:port");
        }
        String id = entry.substring(0, at);
        String host = entry.substring(at + 1, colon);
        int port;
        try {
            port = Integer.parseInt(entry.substring(colon + 1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "bad port in cluster entry '" + entry + "'");
        }
        return new NodeId(id, host, port);
    }
}
