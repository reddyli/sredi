package org.sredi.commands;

import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespInteger;
import org.sredi.resp.RespValue;
import org.sredi.storage.Orchestrator;

public class PublishCommand extends Command {

    private RespBulkString channel;
    private RespBulkString message;

    public PublishCommand() {
        super(Type.PUBLISH);
    }

    @Override
    public void setArgs(RespValue[] args) {
        validateNumArgs(args, len -> len == 3);
        validateArgIsString(args, 1);
        validateArgIsString(args, 2);
        this.channel = args[1].asBulkString();
        this.message = args[2].asBulkString();
    }

    @Override
    public byte[] execute(Orchestrator service) {
        //int delivered = service.publish(channel.getValueAsString(), message.getValue());
        return new RespInteger(1).asResponse();
    }

    @Override
    public String toString() {
        return "PublishCommand [channel=" + channel + "]";
    }
}
