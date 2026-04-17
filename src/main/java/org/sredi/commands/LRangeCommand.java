package org.sredi.commands;

import java.util.List;
import java.util.Map;

import org.sredi.resp.RespArrayValue;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespValue;
import org.sredi.storage.CentralRepository;

public class LRangeCommand extends Command {

    private String key;
    private int start;
    private int stop;

    public LRangeCommand() {
        super(Type.LRANGE);
    }

    @Override
    public void setArgs(RespValue[] args) {
        ArgReader argReader = new ArgReader(type.name(), new String[] {
                ":string", // command name
                ":string", // key
                ":int",    // start
                ":int"     // stop
        });
        Map<String, RespValue> optionsMap = argReader.readArgs(args);
        this.key = optionsMap.get("1").getValueAsString();
        this.start = optionsMap.get("2").getValueAsLong().intValue();
        this.stop = optionsMap.get("3").getValueAsLong().intValue();
    }

    @Override
    public byte[] execute(CentralRepository service) {
        List<String> values = service.lrange(key, start, stop);
        RespValue[] respValues = values.stream()
                .map(v -> new RespBulkString(v.getBytes()))
                .toArray(RespValue[]::new);
        return new RespArrayValue(respValues).asResponse();
    }

    @Override
    public String getKey() { return key; }

    @Override
    public String toString() {
        return "LRangeCommand [key=" + key + ", start=" + start + ", stop=" + stop + "]";
    }
}

