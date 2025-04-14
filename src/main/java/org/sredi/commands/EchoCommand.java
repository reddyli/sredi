package org.sredi.commands;

import org.sredi.storage.CentralRepository;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespConstants;
import org.sredi.resp.RespValue;

public class EchoCommand extends Command {

    RespBulkString bulkStringArg;

    public RespBulkString getEchoValue() {
        return bulkStringArg;
    }

    /**
     * Creates an instance of `EchoCommand` with no arguments.
     */
    public EchoCommand() {
        super(Type.ECHO);
    }

    /**
     * Creates an instance of `EchoCommand` with a bulk string argument.
     *
     * @param bulkStringArg the bulk string argument for the command
     */
    public EchoCommand(RespBulkString bulkStringArg) {
        super(Type.ECHO);
        this.bulkStringArg = bulkStringArg;
    }

    /**
     * Sets the arguments for the command. It validates the number of arguments and ensures that the
     * second argument is a string.
     *
     * @param args the arguments for the command
     * @throws IllegalArgumentException if the number of arguments is not 2 or if the second
     *                                  argument is not a string
     */
    @Override
    public void setArgs(RespValue[] args) {
        validateNumArgs(args, len -> len == 2);
        validateArgIsString(args, 1);

        this.bulkStringArg = args[1].asBulkString();
    }

    @Override
    public byte[] execute(CentralRepository service) {
        return bulkStringArg != null ? bulkStringArg.asResponse() : RespConstants.NULL;
    }

    /**
     * Returns a string representation of the `EchoCommand` object.
     *
     * @return a string representation of the `EchoCommand` object
     */
    @Override
    public String toString() {
        return "EchoCommand [bulkStringArg=" + bulkStringArg + "]";
    }
}
