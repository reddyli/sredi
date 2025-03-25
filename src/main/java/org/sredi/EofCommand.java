package org.sredi;
import org.sredi.commands.RedisCommand;

public class EofCommand extends RedisCommand {

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
