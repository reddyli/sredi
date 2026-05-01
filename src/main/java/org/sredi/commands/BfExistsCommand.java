package org.sredi.commands;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.sredi.resp.RespInteger;
import org.sredi.resp.RespSimpleErrorValue;
import org.sredi.resp.RespValue;
import org.sredi.storage.BloomFilter;
import org.sredi.storage.Orchestrator;

public class BfExistsCommand extends Command {

    private static final String WIRE_NAME = "BF.EXISTS";

    private String key;
    private String item;

    public BfExistsCommand() {
        super(Type.BF_EXISTS);
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
            BloomFilter bf = service.bfGet(key);
            if (bf == null) {
                return new RespInteger(0).asResponse();
            }
            boolean present = bf.mightContain(item.getBytes(StandardCharsets.UTF_8));
            return new RespInteger(present ? 1 : 0).asResponse();
        } catch (IllegalStateException e) {
            return new RespSimpleErrorValue(e.getMessage()).asResponse();
        }
    }

    @Override
    public String getKey() { return key; }

    @Override
    public String toString() {
        return "BfExistsCommand [key=" + key + ", item=" + item + "]";
    }
}
