package org.sredi;

import org.sredi.commands.RedisCommand;

public class TerminateCommand extends RedisCommand {

    public TerminateCommand() {
        super(Type.TERMINATE);
    }

    @Override
    public byte[] execute(CentralRepository service) {
        return null;
    }

    @Override
    public String toString() {
        return "TerminateCommand []";
    }

}
