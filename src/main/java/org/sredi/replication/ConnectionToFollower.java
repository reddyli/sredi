package org.sredi.replication;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sredi.commands.Command;
import org.sredi.commands.ReplConfCommand;
import org.sredi.resp.RespSimpleStringValue;
import org.sredi.resp.RespValue;
import org.sredi.resp.RespValueParser;
import org.sredi.storage.CentralRepository;

public class ConnectionToFollower {
    private static final Logger log = LoggerFactory.getLogger(ConnectionToFollower.class);
    private final LeaderService service;
    @Getter
    private final ClientConnection followerConnection;

    @Setter
    private volatile boolean testingDontWaitForAck = true;

    public ConnectionToFollower(LeaderService service, ClientConnection followerConnection)
            throws IOException {
        this.service = service;
        this.followerConnection = followerConnection;
    }

    public long getTotalReplicationOffset() {
        return service.getTotalReplicationOffset();
    }

    public RespValue sendAndWaitForReplConfAck(long timeoutMillis) throws IOException, InterruptedException {
        ReplConfCommand ack = new ReplConfCommand(ReplConfCommand.Option.GETACK, "*");
        String ackString = new String(ack.asCommand()).toUpperCase();
        log.debug("sendAndWaitForReplConfAck: Sending command {}", ackString.replace("\r\n", "\\r\\n"));
        followerConnection.writeFlush(ackString.getBytes());

        if (testingDontWaitForAck) {
            String response = "REPLCONF ACK 0";
            log.debug("sendAndWaitForReplConfAck: not waiting, hardcoded response: \"{}\"", response);
            return new RespSimpleStringValue(response);
        } else {
            log.debug("sendAndWaitForReplConfAck: waiting for REPLCONF ACK");
            followerConnection.waitForNewValueAvailable(timeoutMillis);
            RespValue response = service.getConnectionManager().getNextValue(followerConnection);
            log.debug("sendAndWaitForReplConfAck: got response from replica: {}", response);
            return response;
        }
    }

    public void sendCommand(Command command) throws IOException {
        ClientConnection clientConnection = getFollowerConnection();
        if (clientConnection.isClosed()) {
            log.warn("Follower connection closed: {}", clientConnection);
            return;
        }
        setTestingDontWaitForAck(false);
        ReplConfAckManager.INSTANCE.setTestingDontWaitForAck(false);
        try {
            clientConnection.writeFlush(command.asCommand());
        } catch (IOException e) {
            log.error("Follower exception during replication connection: {}, exception: {}", clientConnection, e.getMessage());
        }
    }

    @Override
    public String toString() {
        return "ConnectionToFollower: " + followerConnection;
    }

}
