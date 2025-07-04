package org.sredi.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import org.sredi.storage.CentralRepository;
import org.sredi.storage.StoredData;
import org.sredi.resp.RespArrayValue;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespConstants;
import org.sredi.resp.RespValue;

/**
 * Represents a SET command in a server.
 * This class is a subclass of Command and is responsible for setting the command arguments and executing the command.
 */
public class SetCommand extends Command {

    // Args reader specification for SET command
    private static ArgReader ARG_READER = new ArgReader(Type.SET.name(), new String[] {
            ":string", // command name
            ":string", // key
            ":string", // value
            "[nx, xx]",
            "[get]",
            "[ex:int px:int exat:int pxatt:int keepttl]"
    });

    Map<String, RespValue> optionsMap = new HashMap<>();

    /**
     * -- GETTER --
     *  Get the key to set.
     *
     * @return the key
     */
    @Getter
    RespBulkString key;
    /**
     * -- GETTER --
     *  Ge the value to set for the key.
     *
     * @return the value
     */
    @Getter
    RespBulkString value;

    public SetCommand() {
        super(Type.SET);
    }

    public SetCommand(RespBulkString key, RespBulkString value) {
        super(Type.SET);
        this.key = key;
        this.value = value;
    }

    /**
     * Get the options for the command.
     *
     * @return the optionsMap
     */
    Map<String, RespValue> getOptionsMap() {
        return optionsMap;
    }

    /**
     * Sets the arguments for the SET command.
     *
     * This method reads the arguments from the given RespValue array and sets the optionsMap, key, and value variables accordingly.
     *
     * @param args the RespValue array containing the arguments for the SET command
     */
    @Override
    public void setArgs(RespValue[] args) {
        optionsMap = ARG_READER.readArgs(args);
        key = optionsMap.get("1").asBulkString();
        value = optionsMap.get("2").asBulkString();
    }

    @Override
    public byte[] asCommand() {
        List<RespValue> cmdValues = new ArrayList<>();
        cmdValues.add(new RespBulkString(getType().name().getBytes()));
        cmdValues.add(key);
        cmdValues.add(value);

        addCommandOption(cmdValues, "nx");
        addCommandOption(cmdValues, "xx");
        addCommandOption(cmdValues, "get");
        addCommandOption(cmdValues, "ex");
        addCommandOption(cmdValues, "px");
        addCommandOption(cmdValues, "exat");
        addCommandOption(cmdValues, "pxatt");
        addCommandOption(cmdValues, "keppttl");
        return new RespArrayValue(cmdValues.toArray(new RespValue[] {})).asResponse();
    }

    protected void addCommandOption(List<RespValue> cmdValues, String option) {
        if (optionsMap.containsKey(option)) {
            cmdValues.add(new RespBulkString(option.getBytes()));
            if (optionsMap.get(option) != RespConstants.NULL_VALUE) {
                cmdValues.add(
                        new RespBulkString(optionsMap.get(option).getValueAsString().getBytes()));
            }
        }
    }

    /**
     * Executes the SET command in a  server.
     * This method sets the specified key-value pair in the  service according to the options specified in the.
     *
     * If the "xx" option is present, the key is updated only if it already exists.
     * If the "nx" option is present, the key is updated only if it does not already exist.
     * If the "ex" option is present, the key is set to expire after the specified number of seconds.
     * If the "px" option is present, the key is set to expire after the specified number of milliseconds.
     * If the "exat" option is present, the key is set to expire at the specified Unix timestamp.
     * If the "pxat" option is present, the key is set to expire at the specified Unix timestamp.
     * If the "keepttl" option is present, the value is updated, but the expireation time is not changed.
     *
     * @param service the  service to execute the command on
     * @return the response as a byte array:
     *         If the key is not already stored and the "nx" option is present, null is returned as a bulk string response.
     *         If the key is already stored and the "xx" option is present, null is returned as a bulk string response.
     *         If the "get" option is present and there is an existing value, then the previous value is returned as a bulk string response.
     *         Otherwise, OK is returned as a simple string response.
     */
    @Override
    public byte[] execute(CentralRepository service) {
        long now = service.getCurrentTime();
        String keyString = key.getValueAsString();

        // only set if it is NOT already stored in the map
        if (optionsMap.containsKey("nx")) {
            if (service.containsUnexpiredKey(keyString)) {
                return RespConstants.NULL;
            }
        }
        // only set if it is already stored in the map
        if (optionsMap.containsKey("xx")) {
            if (!service.containsUnexpiredKey(keyString)) {
                return RespConstants.NULL;
            }
        }
        boolean doKeepTtl = optionsMap.containsKey("keepttl");
        boolean doGet = optionsMap.containsKey("get");

        Long ttl = getTtl(now);
        StoredData prevData = null;
        if ((doGet || doKeepTtl) && service.containsKey(keyString)) {
            prevData = service.get(keyString);
            ttl = doKeepTtl ? prevData.getTtlMillis() : ttl;
        }
        StoredData storedData = new StoredData(value.getValue(), now, ttl);
        service.set(keyString, storedData);
        return (doGet && prevData != null)
                ? new RespBulkString(prevData.getValue()).asResponse()
                : RespConstants.OK;
    }

    /**
     * Calculates the time-to-live (TTL) value for a  key based on the options map.
     *
     * @param now the current time in milliseconds
     * @return the TTL value in milliseconds, or null if no TTL option is present
     */
    Long getTtl(long now) {
        if (optionsMap.containsKey("ex")) {
            return optionsMap.get("ex").getValueAsLong() * 1000;
        } else if (optionsMap.containsKey("px")) {
            return optionsMap.get("px").getValueAsLong();
        } else if (optionsMap.containsKey("exat")) {
            return optionsMap.get("exat").getValueAsLong() * 1000 - now;
        } else if (optionsMap.containsKey("pxatt")) {
            return optionsMap.get("pxatt").getValueAsLong() - now;
        }
        return null;
    }

    @Override
    public String toString() {
        return "SetCommand [key=" + key + ", value=" + value + "]";
    }

}
