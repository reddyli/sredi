package org.sredi.commands;

import java.util.Map;

import org.sredi.resp.RespInteger;
import org.sredi.resp.RespValue;
import org.sredi.storage.Orchestrator;

public class RPushCommand extends Command {

    private String key;
    private String value;

    public RPushCommand() {
        super(Type.RPUSH);
    }

    @Override
    public void setArgs(RespValue[] args) {
        ArgReader argReader = new ArgReader(type.name(), new String[] {
                ":string", // command name
                ":string", // key
                ":string"  // value
        });
        Map<String, RespValue> optionsMap = argReader.readArgs(args);
        this.key = optionsMap.get("1").getValueAsString();
        this.value = optionsMap.get("2").getValueAsString();
    }

    @Override
    public byte[] execute(Orchestrator service) {
        long size = service.rpush(key, value);
        return new RespInteger(size).asResponse();
    }

    @Override
    public String getKey() { return key; }

    @Override
    public String toString() {
        return "RPushCommand [key=" + key + ", value=" + value + "]";
    }
}

