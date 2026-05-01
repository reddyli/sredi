package org.sredi.commands;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.sredi.resp.RespArrayValue;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespInteger;
import org.sredi.resp.RespSimpleErrorValue;
import org.sredi.resp.RespValue;
import org.sredi.storage.BloomFilter;
import org.sredi.storage.Orchestrator;

public class BfAddCommand extends Command {

    static final long DEFAULT_CAPACITY = 100L;
    static final double DEFAULT_ERROR_RATE = 0.01;

    private static final String WIRE_NAME = "BF.ADD";

    private String key;
    private String item;

    public BfAddCommand() {
        super(Type.BF_ADD);
    }

    @Override
    public void setArgs(RespValue[] args) {
        ArgReader argReader = new ArgReader(WIRE_NAME, new String[] {
                ":string", // command name
                ":string", // key
                ":string"  // item
        });
        Map<String, RespValue> optionsMap = argReader.readArgs(args);
        this.key = optionsMap.get("1").getValueAsString();
        this.item = optionsMap.get("2").getValueAsString();
    }

    @Override
    public byte[] execute(Orchestrator service) {
        try {
            BloomFilter bf = service.bfGetOrCreate(key, DEFAULT_CAPACITY, DEFAULT_ERROR_RATE);
            boolean added = bf.add(item.getBytes(StandardCharsets.UTF_8));
            return new RespInteger(added ? 1 : 0).asResponse();
        } catch (IllegalStateException e) {
            return new RespSimpleErrorValue(e.getMessage()).asResponse();
        }
    }

    @Override
    public byte[] asCommand() {
        return new RespArrayValue(new RespValue[] {
                new RespBulkString(WIRE_NAME.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(key.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(item.getBytes(StandardCharsets.UTF_8))
        }).asResponse();
    }

    @Override
    public String getKey() { return key; }

    @Override
    public String toString() {
        return "BfAddCommand [key=" + key + ", item=" + item + "]";
    }
}
