package org.sredi.commands;

import org.sredi.resp.RespArrayValue;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespInteger;
import org.sredi.resp.RespValue;
import org.sredi.storage.Orchestrator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DelCommand extends Command{

    private RespValue[]  keys;

    public DelCommand() {
        super(Type.DEL);
    }

    @Override
    public void setArgs(RespValue[] args) {
        ArgReader argReader = new ArgReader(type.name(), new String[] { ":string", // command name
                ":var" // keys
        });
        Map<String, RespValue> optionsMap = argReader.readArgs(args);
        this.keys = ((RespArrayValue) optionsMap.get("1")).getValues();
    }

    @Override
    public byte[] execute(Orchestrator service) {
        int deletedKeysCount = 0;
        for(RespValue key : keys) {
            if(service.containsKey(key.getValueAsString())) {
                service.delete(key.getValueAsString());
                deletedKeysCount++;
            }
        }
        return new RespInteger(deletedKeysCount).asResponse();
    }

    @Override
    public byte[] asCommand() {
        List<RespValue> cmdValues = new ArrayList<>(keys.length + 1);
        cmdValues.add(new RespBulkString(getType().name().getBytes()));
        for (RespValue k : keys) {
            cmdValues.add(new RespBulkString(k.getValueAsString().getBytes()));
        }
        return new RespArrayValue(cmdValues.toArray(new RespValue[0])).asResponse();
    }

    @Override
    public String getKey() {
        return keys.length > 0 ? keys[0].getValueAsString() : null;
    }

    @Override
    public String toString() {
        return "";
    }
}
