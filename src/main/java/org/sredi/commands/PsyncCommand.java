package org.sredi.commands;

import java.util.HashMap;
import java.util.Map;

import org.sredi.storage.CentralRepository;
import org.sredi.resp.RespArrayValue;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespInteger;
import org.sredi.resp.RespSimpleStringValue;
import org.sredi.resp.RespValue;

public class PsyncCommand extends Command {

    private static ArgReader ARG_READER = new ArgReader(Type.PSYNC.name(), new String[] { ":string", // command
                                                                                                     // name
            ":string", // first arg
            ":int" // second arg
    });

    private Map<String, RespValue> optionsMap = new HashMap<>();

    public PsyncCommand() {
        super(Type.PSYNC);
    }

    public PsyncCommand(String arg1, Long arg2) {
        super(Type.PSYNC);
        optionsMap.put("0", new RespSimpleStringValue(Type.PSYNC.name()));
        optionsMap.put("1", new RespSimpleStringValue(arg1));
        optionsMap.put("2", new RespInteger(arg2));
    }

    @Override
    protected void setArgs(RespValue[] args) {
        optionsMap = ARG_READER.readArgs(args);
    }

    @Override
    public byte[] execute(CentralRepository service) {
        byte[] psync = service.psync(optionsMap);
        byte[] rdb = service.psyncRdb(optionsMap);
        byte[] response = new byte[psync.length + rdb.length];
        System.arraycopy(psync, 0, response, 0, psync.length);
        System.arraycopy(rdb, 0, response, psync.length, rdb.length);
        return response;
    }

    @Override
    public byte[] asCommand() {
        RespValue[] cmdValues;
        cmdValues = new RespValue[] { new RespBulkString(getType().name().getBytes()),
                new RespBulkString(optionsMap.get("1").getValueAsString().getBytes()),
                new RespBulkString(optionsMap.get("2").getValueAsString().getBytes()) };
        return new RespArrayValue(cmdValues).asResponse();
    }

    @Override
    public String toString() {
        return "PsyncCommand [optionsMap=" + optionsMap + "]";
    }

}
