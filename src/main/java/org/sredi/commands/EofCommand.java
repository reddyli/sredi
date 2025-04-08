package org.sredi.commands;
import org.sredi.storage.CentralRepository;

public class EofCommand extends Command {

    public EofCommand() {
        super(Type.EOF);
    }

    @Override
    public byte[] execute(CentralRepository service) {
        return null;
    }

    @Override
    public String toString() {
        return "EofCommand []";
    }

}
