package org.sredi.commands;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.sredi.storage.Orchestrator;
import org.sredi.resp.RespArrayValue;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespSimpleErrorValue;
import org.sredi.resp.RespValue;
import org.sredi.streams.IllegalStreamItemIdException;
import org.sredi.streams.StreamValue;

public class XreadCommand extends Command {

    private static ArgReader ARG_READER = new ArgReader(Type.XREAD.name(), new String[] {
            ":string", // command name
            "<streams:var>" // streams key required with variable args after it
    });

    private List<String> keys;
    private List<String> startValues;

    public XreadCommand() {
        super(Type.XREAD);
        keys = new ArrayList<>();
        startValues = new ArrayList<>();
    }

    public XreadCommand(List<String> keys, List<String> startValues) {
        super(Type.XREAD);
        this.keys = keys;
        this.startValues = startValues;
    }

    @Override
    public byte[] execute(Orchestrator service) {
        try {
            List<List<StreamValue>> result = service.xread(keys, startValues);
            List<List<RespValue>> resultResp = new ArrayList<>();
            for (int i = 0; i < keys.size(); i++) {
                List<StreamValue> values = result.get(i);
                List<RespValue> respValuesForKey = new ArrayList<>();
                respValuesForKey.add(RespValue.simpleString(keys.get(i)));
                respValuesForKey.add(RespValue.array(values.stream()
                        .map(StreamValue::asRespArrayValue).toArray(RespArrayValue[]::new)));
                resultResp.add(respValuesForKey);
            }
            return RespValue.array(
                    resultResp.stream().map(RespValue::array).toArray(RespValue[]::new))
                    .asResponse();
        } catch (IllegalStreamItemIdException e) {
            return new RespSimpleErrorValue(e.getMessage()).asResponse();
        }
    }

    @Override
    public byte[] asCommand() {
        return new RespArrayValue(
                new RespValue[] {
                        new RespBulkString(getType().name().getBytes()),
                        new RespBulkString("streams".getBytes())
                }).asResponse();
    }

    @Override
    protected void setArgs(RespValue[] args) {
        Map<String, RespValue> optionsMap = ARG_READER.readArgs(args);

        RespArrayValue streams = (RespArrayValue) optionsMap.get("streams");
        RespValue[] valuesArray = streams.getValues();
        if (valuesArray.length == 0 || valuesArray.length % 2 == 1) {
            throw new IllegalArgumentException(
                    String.format("%s: Invalid number of streams pairs", type.name()));
        }
        int n = valuesArray.length / 2;
        for (int i = 0; i < n; i++) {
            keys.add(valuesArray[i].getValueAsString());
            startValues.add(valuesArray[n + i].getValueAsString());
        }
    }

    @Override
    public String getKey() {
        return keys.isEmpty() ? null : keys.get(0);
    }

    @Override
    public String toString() {
        return "XreadCommand [keys=" + keys + ", startValues=" + startValues + "]";
    }

    public List<String> getKeys() {
        return keys;
    }

    public List<String> getStartValues() {
        return startValues;
    }

}
