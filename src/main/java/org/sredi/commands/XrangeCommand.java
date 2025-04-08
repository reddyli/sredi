package org.sredi.commands;

import java.util.List;
import java.util.Map;

import org.sredi.storage.CentralRepository;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespSimpleErrorValue;
import org.sredi.resp.RespValue;
import org.sredi.streams.IllegalStreamItemIdException;
import org.sredi.streams.StreamValue;

public class XrangeCommand extends Command {

    private static ArgReader ARG_READER = new ArgReader(Type.XREAD.name(), new String[] {
            ":string", // command name
            ":string", // key
            ":string", // start id
            ":string" // end id
    });

    private String key;
    private String start;
    private String end;

    public XrangeCommand() {
        super(Type.XRANGE);
    }

    public XrangeCommand(String key, String start, String end) {
        super(Type.XRANGE);
        this.key = key;
        this.start = start;
        this.end = end;
    }

    @Override
    public byte[] execute(CentralRepository service) {
        try {
            List<StreamValue> result = service.xrange(key, start, end);
            RespValue[] resultArray = result.stream().map(StreamValue::asRespArrayValue)
                    .toArray(RespValue[]::new);
            return RespValue.array(resultArray).asResponse();
        } catch (IllegalStreamItemIdException e) {
            return new RespSimpleErrorValue(e.getMessage()).asResponse();
        }
    }

    @Override
    public byte[] asCommand() {
        return RespValue.array(
                new RespBulkString(getType().name().getBytes()),
                new RespBulkString(key.getBytes()),
                new RespBulkString(start.getBytes()),
                new RespBulkString(end.getBytes())).asResponse();
    }

    @Override
    protected void setArgs(RespValue[] args) {
        Map<String, RespValue> optionsMap = ARG_READER.readArgs(args);
        key = optionsMap.get("1").getValueAsString();
        start = optionsMap.get("2").getValueAsString();
        end = optionsMap.get("3").getValueAsString();
    }

    @Override
    public String toString() {
        return "XrangeCommand [key=" + key + ", start=" + start + ", end=" + end + "]";
    }

    public String getKey() {
        return key;
    }

    public String getStart() {
        return start;
    }

    public String getEnd() {
        return end;
    }

}
