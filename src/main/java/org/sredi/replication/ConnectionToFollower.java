package org.sredi.replication;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.Getter;
import lombok.Setter;
import org.sredi.commands.Command;
import org.sredi.commands.ReplConfCommand;
import org.sredi.resp.RespSimpleStringValue;
import org.sredi.resp.RespValue;
import org.sredi.resp.RespValueParser;
import org.sredi.storage.CentralRepository;

public class ConnectionToFollower {
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
        System.out.println(String.format("sendAndWaitForReplConfAck: Sending command %s",
                ackString.replace("\r\n", "\\r\\n")));
        followerConnection.writeFlush(ackString.getBytes());

        if (testingDontWaitForAck) {
            String response = "REPLCONF ACK 0";
            System.out.println(String.format(
                    "sendAndWaitForReplConfAck: not waiting, harcoded response: \"%s\"", response));
            return new RespSimpleStringValue(response);
        } else {
            System.out.println("sendAndWaitForReplConfAck: waiting for REPLCONF ACK");
            followerConnection.waitForNewValueAvailable(timeoutMillis);
            RespValue response = service.getConnectionManager().getNextValue(followerConnection);
            System.out.println(String.format("sendAndWaitForReplConfAck: got response from replica: %s",
                    response));
            return response;
        }
    }

    public void sendCommand(Command command) throws IOException {
        ClientConnection clientConnection = getFollowerConnection();
        if (clientConnection.isClosed()) {
            System.out.printf("Follower connection closed: %s%n", clientConnection);
            return;
        }
        setTestingDontWaitForAck(false);
        ReplConfAckManager.INSTANCE.setTestingDontWaitForAck(false);
        try {
            clientConnection.writeFlush(command.asCommand());
        } catch (IOException e) {
            System.out.printf(
                    "Follower exception during replication connection: %s, exception: %s%n",
                    clientConnection, e.getMessage());
        }
    }

    @Override
    public String toString() {
        return "ConnectionToFollower: " + followerConnection;
    }

}
