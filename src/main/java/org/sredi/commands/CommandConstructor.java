package org.sredi.commands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sredi.resp.RespArrayValue;
import org.sredi.resp.RespType;
import org.sredi.resp.RespValue;

public class CommandConstructor {
    private static final Logger log = LoggerFactory.getLogger(CommandConstructor.class);

    public Command newCommandFromValue(RespValue value) {
        if (value == null) {
            return null;
        }
        if (value.getType() == RespType.ARRAY) {
            return getCommand((RespArrayValue) value);
        } else {
            return getCommand(new RespArrayValue(new RespValue[] { value }));
        }
    }

    Command getCommand(RespArrayValue array) {
        long arrayStartBytesOffset = array.getContext() == null ? 0L
                : array.getContext().getStartBytesOffset();
        String commandName = getCommandName(array.getValues()[0]);
        Command.Type commandType = Command.Type.of(commandName);
        Command command = switch (commandType) {
        case CONFIG -> new ConfigCommand();
        case ECHO -> new EchoCommand();
        case GET -> new GetCommand();
        case INCR -> new IncrCommand();
        case INFO -> new InfoCommand();
        case KEYS -> new KeysCommand();
        case MULTI -> new MultiCommand();
        case EXEC -> new ExecCommand();
        case DISCARD -> new DiscardCommand();
        case PING -> new PingCommand();
        case PSYNC -> new PsyncCommand();
        case REPLCONF -> new ReplConfCommand(array.getContext().getClientConnection(), arrayStartBytesOffset);
        case SET -> new SetCommand();
        case TYPE -> new TypeCommand();
        case WAIT -> new WaitCommand();
        case XADD -> new XaddCommand();
        case XRANGE -> new XrangeCommand();
        case XREAD -> new XreadCommand();
        // special non-standard commands
        case EOF -> new EofCommand();
        case TERMINATE -> new TerminateCommand();
        case null, default -> {
            log.warn("Unknown commandName: {}", commandName);
            yield null;
        }
        };
        if (command != null) {
            command.setArgs(array.getValues());
        }
        return command;
    }

    String getCommandName(RespValue value) {
        String name = value.getValueAsString();
        return name != null ? name.toUpperCase() : null;
    }

}
