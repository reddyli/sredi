package org.sredi.election;

/**
 * Callback invoked on the mesh reader thread for every inbound message.
 * Implementations should not block; offload any slow work to a scheduler.
 *
 * Today there are two implementations:
 *   - ElectionService  (Bully state machine)
 *   - HeartbeatService (failure detector)
 */
@FunctionalInterface
public interface MeshMessageHandler {
    void onMessage(NodeId from, MeshMessage msg);
}
