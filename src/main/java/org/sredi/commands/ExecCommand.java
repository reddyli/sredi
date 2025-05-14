package org.sredi.commands;

import org.sredi.storage.CentralRepository;
import org.sredi.resp.RespArrayValue;
import org.sredi.resp.RespSimpleErrorValue;
import org.sredi.resp.RespValue;
import org.sredi.resp.RespBulkString;

public class ExecCommand extends Command {

    public ExecCommand() {
        super(Type.EXEC);
    }

    @Override
    public byte[] execute(CentralRepository service) {
        try {
            byte[][] results = service.executeTransaction();
            if (results == null) {
                return new RespSimpleErrorValue("EXEC without MULTI").asResponse();
            }
            RespValue[] respResults = new RespValue[results.length];
            for (int i = 0; i < results.length; i++) {
                respResults[i] = new RespBulkString(results[i]);
            }
            return new RespArrayValue(respResults).asResponse();
        } catch (Exception e) {
            return new RespSimpleErrorValue(e.getMessage()).asResponse();
        }
    }

    @Override
    public String toString() {
        return "ExecCommand";
    }
} 