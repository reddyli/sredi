package org.sredi.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.sredi.replication.ClientConnection;
import org.sredi.storage.CentralRepository;
import org.sredi.resp.RespArrayValue;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespConstants;
import org.sredi.resp.RespSimpleStringValue;
import org.sredi.resp.RespValue;

public class ReplConfCommand extends Command {
    public static final String ACK_NAME = "ack";
    public static final String CAPA_NAME = "capa";
    public static final String GETACK_NAME = "getack";
    public static final String LISTENING_PORT_NAME = "listening-port";

    public static enum Option {
        ACK(ACK_NAME), CAPA(CAPA_NAME), GETACK(GETACK_NAME), LISTENING_PORT(LISTENING_PORT_NAME);

        String name;

        Option(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    private static ArgReader ARG_READER = new ArgReader(Type.REPLCONF.name(),
            new String[] { ":string", // command name
                    "[ack:int capa:string getack:string listening-port:int]" });

    private Map<String, RespValue> optionsMap = new HashMap<>();
    private final ClientConnection connection;
    private final long startBytesOffset;

    public ReplConfCommand() {
        this(null, 0L);
    }

    public ReplConfCommand(Option option, String optionValue) {
        this(null, option, optionValue, 0L);
    }

    public ReplConfCommand(ClientConnection conn, long startBytesOffset) {
        super(Type.REPLCONF);
        this.connection = conn;
        this.startBytesOffset = startBytesOffset;
    }

    public ReplConfCommand(ClientConnection conn, Option option, String optionValue, long startBytesOffset) {
        this(conn, startBytesOffset);
        optionsMap.put("0", new RespSimpleStringValue(Type.REPLCONF.name()));
        optionsMap.put(option.getName(), optionValue == null ? RespConstants.NULL_VALUE
                : new RespSimpleStringValue(optionValue));
    }

    @Override
    protected void setArgs(RespValue[] args) {
        optionsMap = ARG_READER.readArgs(args);
    }

    @Override
    public byte[] execute(CentralRepository service) {
        return service.replicationConfirm(connection, optionsMap, startBytesOffset);
    }

    @Override
    public byte[] asCommand() {
        List<RespValue> cmdValues = new ArrayList<>();
        cmdValues.add(new RespBulkString(getType().name().toLowerCase().getBytes()));

        addCommandOption(cmdValues, ACK_NAME);
        addCommandOption(cmdValues, CAPA_NAME);
        addCommandOption(cmdValues, GETACK_NAME);
        addCommandOption(cmdValues, LISTENING_PORT_NAME);
        return new RespArrayValue(cmdValues.toArray(new RespValue[] {})).asResponse();
    }

    protected void addCommandOption(List<RespValue> cmdValues, String option) {
        if (optionsMap.containsKey(option)) {
            cmdValues.add(new RespBulkString(option.getBytes()));
            if (optionsMap.get(option) != RespConstants.NULL_VALUE) {
                cmdValues.add(
                        new RespBulkString(optionsMap.get(option).getValueAsString().getBytes()));
            }
        }
    }

    @Override
    public String toString() {
        return "ReplConfCommand [optionsMap=" + optionsMap + ", connection=" + connection
                + ", startBytesOffset=" + startBytesOffset + "]";
    }

    public Map<String, RespValue> getOptionsMap() {
        return optionsMap;
    }

}
