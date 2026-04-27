package org.sredi.commands;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.sredi.resp.RespArrayValue;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespInteger;
import org.sredi.resp.RespValue;
import org.sredi.storage.Orchestrator;

public class RPushCommand extends Command {

    private String key;
    private List<String> values;

    public RPushCommand() {
        super(Type.RPUSH);
    }

    @Override
    public void setArgs(RespValue[] args) {
        ArgReader argReader = new ArgReader(type.name(), new String[] {
                ":string", // command name
                ":string", // key
                ":var"     // values
        });
        Map<String, RespValue> optionsMap = argReader.readArgs(args);
        this.key = optionsMap.get("1").getValueAsString();
        RespValue[] varValues = ((RespArrayValue) optionsMap.get("2")).getValues();
        this.values = new ArrayList<>(varValues.length);
        for (RespValue v : varValues) {
            this.values.add(v.getValueAsString());
        }
        if (this.values.isEmpty()) {
            throw new IllegalArgumentException("RPUSH: wrong number of arguments");
        }
    }

    @Override
    public byte[] execute(Orchestrator service) {
        long size = 0;
        for (String v : values) {
            size = service.rpush(key, v);
        }
        return new RespInteger(size).asResponse();
    }

    @Override
    public byte[] asCommand() {
        List<RespValue> cmdValues = new ArrayList<>(values.size() + 2);
        cmdValues.add(new RespBulkString(getType().name().getBytes()));
        cmdValues.add(new RespBulkString(key.getBytes()));
        for (String v : values) {
            cmdValues.add(new RespBulkString(v.getBytes()));
        }
        return new RespArrayValue(cmdValues.toArray(new RespValue[0])).asResponse();
    }

    @Override
    public String getKey() { return key; }

    @Override
    public String toString() {
        return "RPushCommand [key=" + key + ", values=" + values + "]";
    }
}

