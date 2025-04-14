package org.sredi.commands;

import java.util.Map;

import org.sredi.storage.CentralRepository;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespValue;

public class TypeCommand extends Command {

    private static ArgReader ARG_READER = new ArgReader(Type.TYPE.name(), new String[] {
            ":string", // command name
            ":string" // key
    });

    private RespBulkString key;

    /**
     * Constructs a new TypeCommand object with the TYPE command type.
     */
    public TypeCommand() {
        super(Type.TYPE);
    }

    /**
     * Constructs a new TypeCommand object with the TYPE command type and the specified key.
     *
     * @param key the key for the TYPE command
     */
    public TypeCommand(RespBulkString key) {
        super(Type.TYPE);
        this.key = key;
    }

    /**
     * Types the key for the TYPE command.
     *
     * @return the key for the TYPE command
     */
    public RespBulkString getKey() {
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
        Map<String, RespValue> optionsMap = ARG_READER.readArgs(args);
        this.key = optionsMap.get("1").asBulkString();
    }


    @Override
    public byte[] execute(CentralRepository service) {
        return service.getType(key.getValueAsString()).asResponse();
    }

    /**
     * Returns a string representation of the TypeCommand object.
     *
     * @return a string representation of the TypeCommand object
     */
    @Override
    public String toString() {
        return "TypeCommand [key=" + key + "]";
    }

}
