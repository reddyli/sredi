package org.sredi.election;

import java.util.Objects;

/**
 * Identity and address of a node participating in the cluster.
 *
 * id        - logical name used for ordering in the Bully algorithm
 *             (the highest-id node wins an election).
 * host/port - the node's CLIENT-facing address (the same port a Redis
 *             client would dial). The mesh listener on each node lives
 *             on port + MESH_PORT_OFFSET.
 */
public final class NodeId implements Comparable<NodeId> {

    public static final int MESH_PORT_OFFSET = 10000;

    private final String id;
    private final String host;
    private final int port;

    public NodeId(String id, String host, int port) {
        this.id = Objects.requireNonNull(id, "id");
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
    }

    public String id() { return id; }
    public String host() { return host; }
    public int port() { return port; }

    public int meshPort() {
        return port + MESH_PORT_OFFSET;
    }

    /**
     * Bully ordering. We prefer numeric comparison when both ids parse as
     * integers, otherwise fall back to lexicographic. This means callers
     * can use either "1, 2, 3" or "nodeA, nodeB, nodeC" without surprise.
     */
    @Override
    public int compareTo(NodeId other) {
        Long a = tryParseLong(this.id);
        Long b = tryParseLong(other.id);
        if (a != null && b != null) {
            return Long.compare(a, b);
        }
        return this.id.compareTo(other.id);
    }

    private static Long tryParseLong(String s) {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NodeId other)) return false;
        return id.equals(other.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return id + "@" + host + ":" + port;
    }
}
