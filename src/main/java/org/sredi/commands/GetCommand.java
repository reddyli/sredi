package org.sredi.commands;

import java.util.Map;

import org.sredi.storage.CentralRepository;
import org.sredi.storage.StoredData;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespConstants;
import org.sredi.resp.RespValue;

/**
 * Represents a GET command in a server. This class is a subclass of Command and is
 * responsible for setting the command arguments and executing the command.
 */
public class GetCommand extends Command {
    private RespBulkString key;

    /**
     * Constructs a new GetCommand object with the GET command type.
     */
    public GetCommand() {
        super(Type.GET);
    }

    /**
     * Constructs a new GetCommand object with the GET command type and the specified key.
     * 
     * @param key the key for the GET command
     */
    public GetCommand(RespBulkString key) {
        super(Type.GET);
        this.key = key;
    }

    /**
     * Gets the key for the GET command.
     * 
     * @return the key for the GET command
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
        ArgReader argReader = new ArgReader(type.name(), new String[] { ":string", // command name
                ":string" // key
        });
        Map<String, RespValue> optionsMap = argReader.readArgs(args);
        this.key = optionsMap.get("1").asBulkString();
    }

    /**
     * Executes the GET command by retrieving the value associated with the specified key from the
     * service. If the key exists and is not expired, the value is returned as a byte array.
     * If the key does not exist or is expired, null is returned.
     * 
     * @param service the service to execute the command on
     * @return the value associated with the key, or null if the key does not exist or is expired
     */
    @Override
    public byte[] execute(CentralRepository service) {
        if (service.containsKey(key.getValueAsString())) {
            StoredData storedData = service.get(key.getValueAsString());
            if (service.isExpired(storedData)) {
                service.delete(key.getValueAsString());
                return RespConstants.NULL;
            }
            return new RespBulkString(storedData.getValue()).asResponse();
        }
        return RespConstants.NULL;
    }

    /**
     * Returns a string representation of the GetCommand object.
     * 
     * @return a string representation of the GetCommand object
     */
    @Override
    public String toString() {
        return "GetCommand [key=" + key + "]";
    }

}
