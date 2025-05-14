package org.sredi.commands;

import org.sredi.storage.CentralRepository;
import org.sredi.resp.RespSimpleStringValue;
import org.sredi.resp.RespSimpleErrorValue;

public class DiscardCommand extends Command {

    public DiscardCommand() {
        super(Type.DISCARD);
    }

    @Override
    public byte[] execute(CentralRepository service) {
        try {
            service.discardTransaction();
            return new RespSimpleStringValue("OK").asResponse();
        } catch (Exception e) {
            return new RespSimpleErrorValue(e.getMessage()).asResponse();
        }
    }

    @Override
    public String toString() {
        return "DiscardCommand";
    }
} 