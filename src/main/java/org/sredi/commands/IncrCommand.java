package org.sredi.commands;

import lombok.Getter;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespValue;
import org.sredi.storage.CentralRepository;
import org.sredi.storage.StoredData;
import org.sredi.storage.StoredDataType;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

@Getter
public class IncrCommand extends Command{

    private RespBulkString key;

    @Override
    public void setArgs(RespValue[] args) {
        ArgReader argReader = new ArgReader(type.name(), new String[] { ":string", // command name
                ":string" // key
        });
        Map<String, RespValue> optionsMap = argReader.readArgs(args);
        this.key = optionsMap.get("1").asBulkString();
    }

    public IncrCommand(Type type) {
        super(type);
    }
    public IncrCommand() {
        super(Type.INCR);
    }

    @Override
    public byte[] execute(CentralRepository service) {
        if(service.containsKey(key.getValueAsString())) {
            StoredData data = service.get(key.getValueAsString());
            if(data.getType() == StoredDataType.STRING) {
                RespBulkString value = new RespBulkString(data.getValue());
                Long currentValue = value.getValueAsLong();
                System.out.println(currentValue);
                System.out.println(this);
            }

        }
        return new byte[2];
    }

    @Override
    public String toString() {
        return "IncrCommand{" +
                "key=" + key +
                '}';
    }
}
