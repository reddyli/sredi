package org.sredi.commands;

import java.util.List;
import java.util.Map;

import org.sredi.storage.CentralRepository;
import org.sredi.resp.RespArrayValue;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespValue;

public class KeysCommand extends Command {
        private String paramString;

    /**
     * Constructs a new KeysCommand object with the KEYS command type.
     */
    public KeysCommand() {
        super(Type.KEYS);
    }

    /**
     * Constructs a new KeysCommand object with the KEYS command type and the specified key.
     *
     * @param key the key for the KEYS command
     */
    public KeysCommand(String paramString) {
        super(Type.KEYS);
        this.paramString = paramString;
    }

    /**
     * Gets the param for the KEYS command.
     *
     * @return the key for the KEYS command
     */
    public String getParam() {
        return paramString;
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
                ":string" // param
        });
        Map<String, RespValue> optionsMap = argReader.readArgs(args);
        this.paramString = optionsMap.get("1").getValueAsString();
    }

    @Override
    public byte[] execute(CentralRepository service) {
        List<RespBulkString> keys = service.getKeys().stream().map(String::getBytes).map(RespBulkString::new).toList();
        return new RespArrayValue(keys.toArray(new RespValue[0])).asResponse();
    }

    /**
     * Returns a string representation of the KeysCommand object.
     *
     * @return a string representation of the KeysCommand object
     */
    @Override
    public String toString() {
        return "KeysCommand [param=" + paramString + "]";
    }

}
