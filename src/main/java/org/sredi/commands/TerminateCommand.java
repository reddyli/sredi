package org.sredi.commands;

import org.sredi.storage.Orchestrator;

public class TerminateCommand extends Command {

    public TerminateCommand() {
        super(Type.TERMINATE);
    }

    @Override
    public byte[] execute(Orchestrator service) {
        return null;
    }

    @Override
    public String toString() {
        return "TerminateCommand []";
    }

}
