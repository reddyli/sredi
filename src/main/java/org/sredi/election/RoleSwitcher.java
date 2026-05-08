package org.sredi.election;

import java.io.IOException;

/**
 * Callback the election service uses to flip the local node's role.
 * Implemented by the Orchestrator; modelled as an interface so the
 * election package doesn't depend on storage.
 */
public interface RoleSwitcher {
    void becomeLeader();
    void becomeFollowerOf(String host, int port) throws IOException;
}
