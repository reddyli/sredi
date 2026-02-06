package org.sredi.replication;

import java.io.IOException;

import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sredi.commands.Command;
import org.sredi.commands.ReplConfCommand;
import org.sredi.resp.RespSimpleStringValue;
import org.sredi.resp.RespValue;

/**
 * Represents the leader's view of a connection to a follower.
 * Used by LeaderService to send replicated commands and request acknowledgments.
 * Each follower that connects via PSYNC gets wrapped in this class.
 */
public class ConnectionToFollower {

    private static final Logger log = LoggerFactory.getLogger(ConnectionToFollower.class);

    private final LeaderService service;

    @Getter
    private final ClientConnection followerConnection;

    // When true, skips waiting for ACK responses (used during initial testing/setup)
    @Setter
    private volatile boolean testingDontWaitForAck = true;

    public ConnectionToFollower(LeaderService service, ClientConnection followerConnection) {
        this.service = service;
        this.followerConnection = followerConnection;
    }

    // Returns the leader's current replication offset
    public long getTotalReplicationOffset() {
        return service.getTotalReplicationOffset();
    }

    // Sends REPLCONF GETACK to follower and waits for ACK response
    public RespValue sendAndWaitForReplConfAck(long timeoutMillis) throws IOException, InterruptedException {
        ReplConfCommand ackRequest = new ReplConfCommand(ReplConfCommand.Option.GETACK, "*");
        String ackString = new String(ackRequest.asCommand()).toUpperCase();
        log.debug("sendAndWaitForReplConfAck: Sending command {}", ackString.replace("\r\n", "\\r\\n"));
        followerConnection.writeFlush(ackString.getBytes());

        if (testingDontWaitForAck) {
            return createTestingResponse();
        }
        return waitForAckResponse(timeoutMillis);
    }

    // Returns hardcoded response when testing mode is enabled
    private RespValue createTestingResponse() {
        String response = "REPLCONF ACK 0";
        log.debug("sendAndWaitForReplConfAck: not waiting, hardcoded response: \"{}\"", response);
        return new RespSimpleStringValue(response);
    }

    // Blocks until follower sends ACK or timeout expires
    private RespValue waitForAckResponse(long timeoutMillis) throws InterruptedException {
        log.debug("sendAndWaitForReplConfAck: waiting for REPLCONF ACK");
        followerConnection.waitForNewValueAvailable(timeoutMillis);
        RespValue response = service.getConnectionManager().getNextValue(followerConnection);
        log.debug("sendAndWaitForReplConfAck: got response from replica: {}", response);
        return response;
    }

    // Sends a command to this follower for replication
    public void sendCommand(Command command) throws IOException {
        if (followerConnection.isClosed()) {
            log.warn("Follower connection closed: {}", followerConnection);
            return;
        }

        // Disable testing mode once real replication starts
        setTestingDontWaitForAck(false);
        ReplConfAckManager.INSTANCE.setTestingDontWaitForAck(false);

        try {
            followerConnection.writeFlush(command.asCommand());
        } catch (IOException e) {
            log.error("Failed to replicate to follower {}: {}", followerConnection, e.getMessage());
        }
    }

    @Override
    public String toString() {
        return "ConnectionToFollower: " + followerConnection;
    }
}
