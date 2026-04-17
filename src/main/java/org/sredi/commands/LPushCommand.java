package org.sredi.commands;

import java.util.Map;

import org.sredi.resp.RespInteger;
import org.sredi.resp.RespValue;
import org.sredi.storage.CentralRepository;

public class LPushCommand extends Command {

    private String key;
    private String value;

    public LPushCommand() {
        super(Type.LPUSH);
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
    public byte[] execute(CentralRepository service) {
        long size = service.lpush(key, value);
        return new RespInteger(size).asResponse();
    }

    @Override
    public String getKey() { return key; }

    @Override
    public String toString() {
        return "LPushCommand [key=" + key + ", value=" + value + "]";
    }
}
