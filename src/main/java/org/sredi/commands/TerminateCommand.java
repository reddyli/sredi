package org.sredi.commands;

import org.sredi.storage.CentralRepository;

public class TerminateCommand extends Command {

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
