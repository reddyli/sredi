package org.sredi.commands;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.sredi.resp.RespArrayValue;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespConstants;
import org.sredi.resp.RespSimpleErrorValue;
import org.sredi.resp.RespValue;
import org.sredi.storage.Orchestrator;

public class BfReserveCommand extends Command {

    private static final String WIRE_NAME = "BF.RESERVE";

    private String key;
    private double errorRate;
    private long capacity;

    public BfReserveCommand() {
        super(Type.BF_RESERVE);
    }

    @Override
    public void setArgs(RespValue[] args) {
        ArgReader argReader = new ArgReader(WIRE_NAME, new String[] {
                ":string", // command name
                ":string", // key
                ":string", // error_rate (parsed as double)
                ":int"     // capacity
        });
        Map<String, RespValue> optionsMap = argReader.readArgs(args);
        this.key = optionsMap.get("1").getValueAsString();
        String errorRateStr = optionsMap.get("2").getValueAsString();
        try {
            this.errorRate = Double.parseDouble(errorRateStr);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    WIRE_NAME + ": Invalid error rate at index 2: " + errorRateStr);
        }
        this.capacity = optionsMap.get("3").getValueAsLong();
    }

    @Override
    public byte[] execute(Orchestrator service) {
        try {
            service.bfReserve(key, capacity, errorRate);
            return RespConstants.OK;
        } catch (IllegalStateException | IllegalArgumentException e) {
            return new RespSimpleErrorValue(e.getMessage()).asResponse();
        }
    }

    @Override
    public byte[] asCommand() {
        return new RespArrayValue(new RespValue[] {
                new RespBulkString(WIRE_NAME.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(key.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(String.valueOf(errorRate).getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(String.valueOf(capacity).getBytes(StandardCharsets.UTF_8))
        }).asResponse();
    }

    @Override
    public String getKey() { return key; }

    @Override
    public String toString() {
        return "BfReserveCommand [key=" + key + ", errorRate=" + errorRate + ", capacity=" + capacity + "]";
    }
}
