package org.sredi.commands;

import org.sredi.storage.CentralRepository;
import org.sredi.resp.RespSimpleStringValue;
import org.sredi.resp.RespConstants;

public class MultiCommand extends Command {

    public MultiCommand() {
        super(Type.MULTI);
    }

    @Override
    public byte[] execute(CentralRepository service) {
        service.startTransaction();
        return new RespSimpleStringValue("OK").asResponse();
    }

    @Override
    public String toString() {
        return "MultiCommand";
    }
} 