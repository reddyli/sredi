package org.sredi.commands;

import java.util.Map;

import org.sredi.resp.RespConstants;
import org.sredi.resp.RespSimpleErrorValue;
import org.sredi.resp.RespValue;
import org.sredi.storage.CentralRepository;

public class AuthCommand extends Command {

    private String password;

    public AuthCommand() {
        super(Type.AUTH);
    }

    @Override
    protected void setArgs(RespValue[] args) {
        ArgReader argReader = new ArgReader(type.name(), new String[] {
                ":string", // command name
                ":string"  // password
        });
        Map<String, RespValue> optionsMap = argReader.readArgs(args);
        this.password = optionsMap.get("1").getValueAsString();
    }

    @Override
    public byte[] execute(CentralRepository service) {
        if (service.authenticate(password)) {
            return RespConstants.OK;
        }
        return new RespSimpleErrorValue("ERR invalid password").asResponse();
    }

    @Override
    public String toString() {
        return "AuthCommand";
    }
}
