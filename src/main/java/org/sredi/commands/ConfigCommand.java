package org.sredi.commands;

import java.util.Map;

import org.sredi.storage.CentralRepository;
import org.sredi.resp.RespArrayValue;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespValue;

public class ConfigCommand extends Command {
    private Action action;
    private String key;

    enum Action {
        GET, SET, RESETSTAT
    }

    /**
     * Constructs a new ConfigCommand object with no args.
     */
    public ConfigCommand() {
        super(Type.CONFIG);
    }

    /**
     * Constructs a new ConfigCommand object with the specified action and key.
     *
     * @param action of the command
     * @param key    the key for the action
     */
    public ConfigCommand(Action action, String key) {
        super(Type.CONFIG);
        this.action = action;
        this.key = key;
    }

    /**
     * Gets the action for the CONFIG command.
     *
     * @return the action for the CONFIG command
     */
    public Action getAction() {
        return action;
    }

    /**
     * Gets the key for the CONFIG command.
     *
     * @return the key for the CONFIG command
     */
    public String getKey() {
        return key;
    }

    /**
     * Sets the command arguments by parsing the provided RespValue array. The arguments should
     * contain the key as the first element.
     *
     * @param args the command arguments
     */
    @Override
    public void setArgs(RespValue[] args) {
        ArgReader argReader = new ArgReader(type.name(), new String[] { ":string", // command name
                "[get:string set:string resetstat:string]" // action and key
        });
        Map<String, RespValue> optionsMap = argReader.readArgs(args);
        if (optionsMap.containsKey("get")) {
            action = Action.GET;
            key = optionsMap.get("get").getValueAsString();
        } else if (optionsMap.containsKey("set")) {
            action = Action.SET;
            key = optionsMap.get("set").getValueAsString();
        } else if (optionsMap.containsKey("resetstat")) {
            action = Action.RESETSTAT;
            key = optionsMap.get("resetstat").getValueAsString();
        } else {
            throw new IllegalArgumentException("Unknown CONFIG command action. arguments: " + args);
        }
    }

    @Override
    public byte[] execute(CentralRepository service) {
        switch (action) {
        case GET:
            return new RespArrayValue(new RespValue[] { new RespBulkString(key.getBytes()),
                    new RespBulkString(service.getConfig(key).getBytes()) }).asResponse();
        default:
            throw new UnsupportedOperationException(String
                    .format("Action %s not yet implemented for CONFIG command: %s", action, this));
        }
    }

    public byte[] asCommand() {
        return new RespArrayValue(new RespValue[] { new RespBulkString(type.name().toLowerCase().getBytes()),
                new RespBulkString(action.name().toLowerCase().getBytes()), new RespBulkString(key.getBytes()) })
                        .asResponse();
    }

    @Override
    public String toString() {
        return "ConfigCommand [action=" + action + ", key=" + key + "]";
    }

}
