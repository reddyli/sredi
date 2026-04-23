package org.sredi.commands;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.sredi.resp.RespArrayValue;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespInteger;
import org.sredi.resp.RespValue;
import org.sredi.storage.Orchestrator;

public class SubscribeCommand extends Command {

    private RespValue[] channels;

    public SubscribeCommand() {
        super(Type.SUBSCRIBE);
    }

    @Override
    public void setArgs(RespValue[] args) {
        validateNumArgs(args, len -> len >= 2);
        channels = new RespValue[args.length - 1];
        System.arraycopy(args, 1, channels, 0, channels.length);
    }

    @Override
    public byte[] execute(Orchestrator service) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            for (RespValue channel : channels) {
                String name = channel.getValueAsString();
                //int count = service.subscribe(name);
                out.write(buildReply(name, 1));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }

    static byte[] buildReply(String channel, int count) {
        RespValue[] parts = new RespValue[] {
                new RespBulkString("subscribe".getBytes()),
                new RespBulkString(channel.getBytes()),
                new RespInteger(count)
        };
        return new RespArrayValue(parts).asResponse();
    }

    @Override
    public String toString() {
        return "SubscribeCommand [channels=" + channels.length + "]";
    }
}
