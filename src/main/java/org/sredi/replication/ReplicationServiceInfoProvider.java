package org.sredi.replication;

/**
 * Interface for services that can provide replication status information.
 * Used by the INFO command to report replication state.
 * Implemented by both LeaderService and FollowerService.
 */
public interface ReplicationServiceInfoProvider {

    // Appends replication info (e.g., master_replid, master_repl_offset) to the builder
    void getReplicationInfo(StringBuilder sb);
}
