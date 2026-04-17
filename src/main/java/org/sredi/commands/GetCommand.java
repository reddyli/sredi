package org.sredi.commands;

import java.util.Map;

import lombok.Getter;
import org.sredi.storage.CentralRepository;
import org.sredi.storage.StoredData;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespConstants;
import org.sredi.resp.RespValue;

/**
 * Represents a GET command in a server. This class is a subclass of Command and is
 * responsible for setting the command arguments and executing the command.
 */
@Getter
public class GetCommand extends Command {

    private RespBulkString key;

    public GetCommand() {
        super(Type.GET);
    }

    public GetCommand(RespBulkString key) {
        super(Type.GET);
        this.key = key;
    }

    @Override
    public void setArgs(RespValue[] args) {
        ArgReader argReader = new ArgReader(type.name(), new String[] { ":string", // command name
                ":string" // key
        });
        Map<String, RespValue> optionsMap = argReader.readArgs(args);
        this.key = optionsMap.get("1").asBulkString();
    }

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

    @Override
    public String getKey() {
        return key.getValueAsString();
    }

    @Override
    public String toString() {
        return "GetCommand [key=" + key + "]";
    }

}
