package org.sredi.replication;

import java.io.IOException;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;

import lombok.Getter;
import org.sredi.commands.Command;
import org.sredi.commands.PingCommand;
import org.sredi.commands.PsyncCommand;
import org.sredi.commands.ReplConfCommand;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespValue;
import org.sredi.resp.RespValueParser;

public class ConnectionToLeader {
    // keep a list of socket connections and continue checking for new connections
    private final FollowerService service;
    private final ClientConnection leaderConnection;
    private final Deque<CommandAndResponseConsumer> commandsToLeader = new ConcurrentLinkedDeque<>();
    private final ExecutorService executor;
    private final RespValueParser valueParser;
    private volatile boolean done = false;
    @Getter
    private long handshakeBytesReceived = 0;
    private RespBulkString fullResyncRdb;

    public ConnectionToLeader(FollowerService service) throws IOException {
        this.service = service;
        executor = Executors.newFixedThreadPool(1); // We need just one thread for sending commands
                                                    // to the leader
        valueParser = new RespValueParser();

        leaderConnection = new ClientConnection(service.getLeaderClientSocket(), valueParser);
        System.out.println(String.format("Connection to leader: %s, isOpened: %s", leaderConnection,
                !leaderConnection.isClosed()));

        // create the thread for sending handshake commands to the leader
        executor.execute(() -> {
            try {
                runHandshakeLoop();
            } catch (InterruptedException e) {
                System.out
                        .println("InterruptedException on send command thread: " + e.getMessage());
                terminate();
            }
        });
    }

    public void startHandshake() {
        System.out.println("Starting handshake with leader");
        sendCommand(new PingCommand(), (cmd, response) -> {
            ReplConfCommand conf1 = new ReplConfCommand(ReplConfCommand.Option.LISTENING_PORT,
                    String.valueOf(service.getPort()));
            sendCommand(conf1, (conf1Cmd, response2) -> {
                ReplConfCommand conf2 = new ReplConfCommand(ReplConfCommand.Option.CAPA, "psync2");
                sendCommand(conf2, (conf2Cmd, response3) -> {
                    PsyncCommand psync = new PsyncCommand("?", -1L);
                    sendCommand(psync, (psyncCmd, response4) -> {
                        if (response4.isSimpleString() && response4.getValueAsString().toUpperCase()
                                .startsWith("FULLRESYNC")) {
                            System.out.println("Full resync - looking for rdb response");
                            return true;
                        }
                        if (!response4.isBulkString()) {
                            throw new RuntimeException(
                                    String.format("Unexpected response: %s", response4));
                        }
                        setFullResyncRdb((RespBulkString) response4);
                        System.out.println("Handshake completed");
                        handshakeBytesReceived = leaderConnection.getNumBytesReceived();

                        // after the handshake, allow the ConnectionManager to poll for commands
                        // from the leader and process them in the FollowerService on the main event
                        // loop
                        service.getConnectionManager().addPriorityConnection(leaderConnection);
                        return false;
                    });
                    return false;
                });
                return false;
            });
            return false;
        });
    }

    private void setFullResyncRdb(RespBulkString fullResyncRdb) {
        this.fullResyncRdb = fullResyncRdb;
    }

    public byte[] getFullResyncRdb() {
        return fullResyncRdb.getValue();
    }

    private void sendCommand(Command command,
                             BiFunction<Command, RespValue, Boolean> responseConsumer) {
        CommandAndResponseConsumer cmd = new CommandAndResponseConsumer(command, responseConsumer);
        // add the command to the queue
        commandsToLeader.offerLast(cmd);
    }

    public void terminate() {
        System.out.printf("Terminate follower invoked. Closing socket to leader %s.%n",
                service.getLeaderClientSocket());
        done = true;
        // close the connection to the leader
        try {
            service.getLeaderClientSocket().close();
        } catch (IOException e) {
            System.out.println("IOException on socket close: " + e.getMessage());
        }
        // executor close - waits for thread to finish
        executor.close();
    }

    public void runHandshakeLoop() throws InterruptedException {
        while (!done) {
            if (leaderConnection.isClosed()) {
                System.out.printf(
                        "Terminating repository due to connection is closed by leader during handshake: %s%n",
                        leaderConnection);
                terminate();
                continue;
            }

            // check for handshake commands waiting to be sent
            try {
                while (!commandsToLeader.isEmpty()) {
                    CommandAndResponseConsumer cmd = commandsToLeader.pollFirst();
                    // send the command to the leader
                    System.out.printf("Sending leader command: %s%n", cmd.command);
                    leaderConnection.writeFlush(cmd.command.asCommand());

                    // read the response - will wait on the stream until the whole value is parsed
                    RespValue response = leaderConnection.readValue();
                    System.out.printf("Received leader response: %s%n", response);

                    // responseConsumer returns True if we expect the RDB value from the command
                    if (cmd.responseConsumer.apply(cmd.command, response)) {
                        try {
                            byte[] rdb = leaderConnection.readRDB();

                            response = new RespBulkString(rdb);
                            System.out.printf("Received leader RDB: %s%n", response);
                            cmd.responseConsumer.apply(cmd.command, response);
                        } catch (IOException e) {
                            System.out.printf(
                                    "ConnectionToLeader: IOException on readRDB: %s %s%n",
                                    e.getClass().getSimpleName(), e.getMessage());
                        }
                    }
                }
                // sleep a bit before the next handshake command
                Thread.sleep(50L);
            } catch (Exception e) {
                System.out.printf("ConnectionToLeader Loop Exception: %s \"%s\"%n",
                        e.getClass().getSimpleName(), e.getMessage());
            }
        }
        System.out.printf(
                "Exiting thread for handshake commands - done: %s%n", done);
    }

    public boolean isLeaderConnection(ClientConnection conn) {
        return leaderConnection.equals(conn);
    }

    public void executeCommandFromLeader(ClientConnection conn, Command command)
            throws IOException {
        if (!isLeaderConnection(conn)) {
            System.out.printf(
                    "ConnectionToLeader ERROR: executeCommandFromLeader called with non-leader connection: %s%n",
                    conn);
            return;
        }

        if (command.isReplicatedCommand()) {
            System.out.printf("Received replicated command from leader: %s%n", conn);
        } else {
            System.out.printf("Received request from leader: %s%n", conn);
        }

        // if the command came from the leader, then for most commands the leader does not
        // expect a response
        boolean writeResponse = shouldSendResponseToConnection(command, conn);

        byte[] response = command.execute(service);
        if (writeResponse) {
            System.out.printf("Follower service sending %s response: %s%n",
                    command.getType().name(), Command.responseLogString(response));
            if (response != null && response.length > 0) {
                conn.writeFlush(response);
            }
        } else {
            System.out.printf("Follower service do not send %s response: %s%n",
                    command.getType().name(), Command.responseLogString(response));
        }
    }

    public boolean shouldSendResponseToConnection(Command command, ClientConnection conn) {
        if (!leaderConnection.equals(conn)) {
            return true;
        }
        return command instanceof ReplConfCommand && ((ReplConfCommand) command)
                .getOptionsMap().containsKey(ReplConfCommand.GETACK_NAME);
    }

    private static class CommandAndResponseConsumer {
        private final Command command;
        private final BiFunction<Command, RespValue, Boolean> responseConsumer;

        public CommandAndResponseConsumer(Command command,
                                          BiFunction<Command, RespValue, Boolean> responseConsumer) {
            this.command = command;
            this.responseConsumer = responseConsumer;
        }
    }
}
