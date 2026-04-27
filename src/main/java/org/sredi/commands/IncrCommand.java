package org.sredi.commands;

import lombok.Getter;
import org.sredi.resp.RespArrayValue;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespInteger;
import org.sredi.resp.RespValue;
import org.sredi.storage.Orchestrator;
import org.sredi.storage.DataEntry;
import org.sredi.storage.DataEntryType;

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
    public IncrCommand() {
        super(Type.INCR);
    }

    @Override
    public byte[] execute(Orchestrator service) {
        String keyString = key.getValueAsString();
        long now = service.getCurrentTime();
        
        if(service.containsKey(keyString)) {
            DataEntry data = service.get(keyString);
            if(data.getType() == DataEntryType.STRING) {
                RespBulkString value = new RespBulkString(data.getValue());
                Long currentValue = value.getValueAsLong();
                
                if(currentValue == null) {
                    // If value cannot be parsed as a number, set it to 0
                    DataEntry newData = new DataEntry("0".getBytes(), now, data.getTtlMillis());
                    service.set(keyString, newData);
                    return new RespInteger(0).asResponse();
                } else {
                    // Increment the value by 1
                    long newValue = currentValue + 1;
                    DataEntry newData = new DataEntry(String.valueOf(newValue).getBytes(), now, data.getTtlMillis());
                    service.set(keyString, newData);
                    return new RespInteger(newValue).asResponse();
                }
            }
        } else {
            // Key doesn't exist, create it with value 0
            DataEntry newData = new DataEntry("0".getBytes(), now, null);
            service.set(keyString, newData);
            return new RespInteger(0).asResponse();
        }
        
        return new RespInteger(0).asResponse();
    }

    @Override
    public byte[] asCommand() {
        return new RespArrayValue(new RespValue[] {
                new RespBulkString(getType().name().getBytes()),
                key
        }).asResponse();
    }

    @Override
    public String getKey() {
        return key.getValueAsString();
    }

    @Override
    public String toString() {
        return "IncrCommand{" +
                "key=" + key +
                '}';
    }
}
