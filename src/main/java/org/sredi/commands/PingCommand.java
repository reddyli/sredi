package org.sredi.commands;

import org.sredi.storage.Orchestrator;
import org.sredi.resp.RespArrayValue;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespValue;

public class PingCommand extends Command {

    public PingCommand() {
        super(Type.PING);
    }

    @Override
    public byte[] execute(Orchestrator service) {
        return "+PONG\r\n".getBytes();
    }

    @Override
    public byte[] asCommand() {
        return new RespArrayValue(
                new RespValue[] { new RespBulkString(getType().name().getBytes()) }).asResponse();
    }

    @Override
    public String toString() {
        return "PingCommand";
    }
}
