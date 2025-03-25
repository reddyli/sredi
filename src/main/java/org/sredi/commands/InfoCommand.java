package org.sredi.commands;

import java.util.Map;

import org.sredi.CentralRepository;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespSimpleStringValue;
import org.sredi.resp.RespValue;

public class InfoCommand extends RedisCommand {

    private static ArgReader ARG_READER = new ArgReader(Type.INFO.name(), new String[] {
            ":string", // command name
            "[server]",
            "[clients]",
            "[memory]",
            "[persistence]",
            "[stats]",
            "[replication]",
            "[cpu]",
            "[commandstats]",
            "[latencystats]",
            "[sentinel]",
            "[cluster]",
            "[modules]",
            "[keyspace]",
            "[errorstats]",
            "[all]",
            "[default]",
            "[everything]"
    });

    private Map<String, RespValue> optionsMap = Map.of(
        "0", new RespSimpleStringValue(Type.INFO.name()));

    public InfoCommand() {
        super(Type.INFO);
    }

    @Override
    protected void setArgs(RespValue[] args) {
        optionsMap = ARG_READER.readArgs(args);
    }

    @Override
    public byte[] execute(CentralRepository service) {
        return new RespBulkString(service.info(optionsMap).getBytes()).asResponse();
    }

    @Override
    public String toString() {
        return "InfoCommand [optionsMap=" + optionsMap + "]";
    }

}
