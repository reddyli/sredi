package org.sredi.commands;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.sredi.storage.CentralRepository;
import org.sredi.resp.RespValue;

public abstract class Command {
    public enum Type {
        CONFIG, DEL, ECHO, GET, INCR, INFO, KEYS, MULTI, EXEC, DISCARD, PING, PSYNC, REPLCONF, SET, TYPE, WAIT, XADD, XRANGE,
        XREAD,
        EOF, // close a client connection
        TERMINATE; // close all connections and kill the server

        static Type of(String command) {
            try {
                return Type.valueOf(command);
            } catch (Exception e) {
                return null;
            }
        }
    }

    protected final Type type;

    public Command(Type type) {
        this.type = type;
    }

    public Type getType() {
        return type;
    }

    public boolean isReplicatedCommand() {
        return type == Type.SET || type == Type.DEL;
    }

    public boolean isBlockingCommand() {
        return false;
    }

    protected void setArgs(RespValue[] args) {
        // ignore by default
    }

    protected void validateNumArgs(RespValue[] args,
            Function<Integer, Boolean> validLengthCondition) {
        if (!validLengthCondition.apply(args.length)) {
            throw new IllegalArgumentException(
                    String.format("%s: Invalid number of arguments: %d", type.name(), args.length));
        }
    }

    protected void validateArgIsString(RespValue[] args, int index) {
        RespValue arg = index < args.length ? args[index] : null;
        if (arg == null || (!arg.isBulkString() && !arg.isSimpleString())) {
            throw new IllegalArgumentException(String
                    .format("%s: Invalid arg, expected string. %d: %s", type.name(), index, arg));
        }
    }

    public void validateArgIsInteger(RespValue[] args, int index) {
        RespValue arg = args[index];
        if (arg.getValueAsLong() == null) {
            throw new IllegalArgumentException(String
                    .format("%s: Invalid arg, expected integer %d: %s", type.name(), index, arg));
        }
    }

    public void validateArgForStateTransition(RespValue[] args, int i, int state, int nextState,
            Map<Integer, List<Integer>> transitions) {
        if (nextState == -1 || !transitions.get(state).contains(nextState)) {
            throw new IllegalArgumentException(
                    String.format("%s: Invalid or missing argument at index. %d, %s", type.name(),
                            i, Arrays.toString(args)));
        }
    }

    public abstract byte[] execute(CentralRepository service);

    public abstract String toString();

    public byte[] asCommand() {
        // must be overridden by subclass in order to send the command to the leader or follower
        throw new UnsupportedOperationException("Unimplemented method 'asCommand'");
    }

    public static String responseLogString(byte[] response) {
        return response == null ? null : new String(response).replace("\r\n", "\\r\\n");
    }

}
