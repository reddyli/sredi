package org.sredi.commands;

import java.util.Map;

import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespConstants;
import org.sredi.resp.RespValue;
import org.sredi.storage.Orchestrator;

public class LPopCommand extends Command {

    private String key;

    public LPopCommand() {
        super(Type.LPOP);
    }

    @Override
    public void setArgs(RespValue[] args) {
        ArgReader argReader = new ArgReader(type.name(), new String[] {
                ":string", // command name
                ":string"  // key
        });
        Map<String, RespValue> optionsMap = argReader.readArgs(args);
        this.key = optionsMap.get("1").getValueAsString();
    }

    @Override
    public byte[] execute(Orchestrator service) {
        String value = service.lpop(key);
        if (value == null) return RespConstants.NULL;
        return new RespBulkString(value.getBytes()).asResponse();
    }

    @Override
    public String getKey() { return key; }

    @Override
    public String toString() {
        return "LPopCommand [key=" + key + "]";
    }
}

