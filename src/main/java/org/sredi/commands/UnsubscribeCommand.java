package org.sredi.commands;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.sredi.resp.RespArrayValue;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespConstants;
import org.sredi.resp.RespInteger;
import org.sredi.resp.RespValue;
import org.sredi.storage.Orchestrator;

public class UnsubscribeCommand extends Command {

    private RespValue[] channels;

    public UnsubscribeCommand() {
        super(Type.UNSUBSCRIBE);
    }

    @Override
    public void setArgs(RespValue[] args) {
        validateNumArgs(args, len -> len >= 1);
        channels = new RespValue[args.length - 1];
        System.arraycopy(args, 1, channels, 0, channels.length);
    }

    @Override
    public byte[] execute(Orchestrator service) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            if (channels.length == 0) {
                //List<String> removed = service.unsubscribeAll();
//                if (removed.isEmpty()) {
//                    out.write(buildReply(null, 0));
//                } else {
//                    int remaining = removed.size();
//                    for (String channel : removed) {
//                        remaining--;
//                        out.write(buildReply(channel, remaining));
//                    }
//                }
            } else {
                for (RespValue channel : channels) {
                    String name = channel.getValueAsString();
                    //int count = service.unsubscribe(name);
                    out.write(buildReply(name, 1));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }

    static byte[] buildReply(String channel, int count) {
        RespValue channelPart = channel == null
                ? RespConstants.NULL_VALUE
                : new RespBulkString(channel.getBytes());
        RespValue[] parts = new RespValue[] {
                new RespBulkString("unsubscribe".getBytes()),
                channelPart,
                new RespInteger(count)
        };
        return new RespArrayValue(parts).asResponse();
    }

    @Override
    public String toString() {
        return "UnsubscribeCommand [channels=" + channels.length + "]";
    }
}
