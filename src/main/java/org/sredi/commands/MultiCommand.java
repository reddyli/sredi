package org.sredi.commands;

import org.sredi.storage.Orchestrator;
import org.sredi.resp.RespSimpleStringValue;
import org.sredi.resp.RespConstants;

public class MultiCommand extends Command {

    public MultiCommand() {
        super(Type.MULTI);
    }

    @Override
    public byte[] execute(Orchestrator service) {
        service.startTransaction();
        return new RespSimpleStringValue("OK").asResponse();
    }

    @Override
    public String toString() {
        return "MultiCommand";
    }
} 