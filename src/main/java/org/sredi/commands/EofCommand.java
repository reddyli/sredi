package org.sredi.commands;
import org.sredi.storage.Orchestrator;

public class EofCommand extends Command {

    public EofCommand() {
        super(Type.EOF);
    }

    @Override
    public byte[] execute(Orchestrator service) {
        return null;
    }

    @Override
    public String toString() {
        return "EofCommand []";
    }

}
