package org.sredi.commands;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import lombok.Getter;
import org.sredi.storage.Orchestrator;
import org.sredi.resp.RespValue;

@Getter
public abstract class Command {

    public enum Type {
        AUTH, CONFIG, DEL, ECHO, GET, INCR, INFO, KEYS, LPUSH, RPUSH, LPOP, RPOP, LRANGE, MULTI, EXEC, DISCARD, PING, PSYNC, PUBLISH, REPLCONF, SET, SUBSCRIBE, TYPE, UNSUBSCRIBE, WAIT, XADD, XRANGE,
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

        private static final Set<Type> WRITE_COMMANDS = Set.of(
                SET, DEL, INCR, LPUSH, RPUSH, LPOP, RPOP, XADD
        );

        public boolean isWrite() {
            return WRITE_COMMANDS.contains(this);
        }
    }

    protected final Type type;

    public Command(Type type) {
        this.type = type;
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

    // Returns the key this command operates on, or null for key-less commands
    public String getKey() {
        return null;
    }

    public abstract byte[] execute(Orchestrator service);

    public abstract String toString();

    public byte[] asCommand() {
        // must be overridden by subclass in order to send the command to the leader or follower
        throw new UnsupportedOperationException("Unimplemented method 'asCommand'");
    }

    public static String responseLogString(byte[] response) {
        return response == null ? null : new String(response).replace("\r\n", "\\r\\n");
    }

}
