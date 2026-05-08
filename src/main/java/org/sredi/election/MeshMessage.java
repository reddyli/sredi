package org.sredi.election;

/**
 * Wire messages exchanged on the cluster mesh (control plane).
 *
 * Encoding is a single line of space-separated tokens followed by '\n':
 *   HELLO       <senderId>
 *   HEARTBEAT   <leaderId> <epoch>
 *   ELECTION    <senderId> <epoch>
 *   OK          <senderId> <epoch>
 *   COORDINATOR <leaderId> <epoch>
 *
 * Kept text-based on purpose: easy to inspect with a tcpdump or netcat,
 * and tiny enough that we don't need framing headaches.
 */
public sealed interface MeshMessage
        permits MeshMessage.Hello,
                MeshMessage.Heartbeat,
                MeshMessage.Election,
                MeshMessage.Ok,
                MeshMessage.Coordinator {

    String encode();

    /** Sent as the first message on every newly-opened peer link. */
    record Hello(String senderId) implements MeshMessage {
        @Override public String encode() { return "HELLO " + senderId; }
    }

    /** Periodic liveness signal from the current leader. */
    record Heartbeat(String leaderId, long epoch) implements MeshMessage {
        @Override public String encode() { return "HEARTBEAT " + leaderId + " " + epoch; }
    }

    /** Bully election probe: "anyone higher than me alive?" */
    record Election(String senderId, long epoch) implements MeshMessage {
        @Override public String encode() { return "ELECTION " + senderId + " " + epoch; }
    }

    /** Reply to an ELECTION from a higher-id node: "I'm here, stand down." */
    record Ok(String senderId, long epoch) implements MeshMessage {
        @Override public String encode() { return "OK " + senderId + " " + epoch; }
    }

    /** Broadcast by the winner of an election. */
    record Coordinator(String leaderId, long epoch) implements MeshMessage {
        @Override public String encode() { return "COORDINATOR " + leaderId + " " + epoch; }
    }

    /** Parse one wire line into a typed message; null on malformed input. */
    static MeshMessage parse(String line) {
        if (line == null) return null;
        String trimmed = line.trim();
        if (trimmed.isEmpty()) return null;

        String[] tok = trimmed.split(" ");
        try {
            return switch (tok[0]) {
                case "HELLO" -> tok.length == 2 ? new Hello(tok[1]) : null;
                case "HEARTBEAT" -> tok.length == 3
                        ? new Heartbeat(tok[1], Long.parseLong(tok[2])) : null;
                case "ELECTION" -> tok.length == 3
                        ? new Election(tok[1], Long.parseLong(tok[2])) : null;
                case "OK" -> tok.length == 3
                        ? new Ok(tok[1], Long.parseLong(tok[2])) : null;
                case "COORDINATOR" -> tok.length == 3
                        ? new Coordinator(tok[1], Long.parseLong(tok[2])) : null;
                default -> null;
            };
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
