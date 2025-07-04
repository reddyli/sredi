package org.sredi.resp;
public enum RespType {
    SIMPLE_STRING('+'),
    SIMPLE_ERROR('-'),
    INTEGER(':'),
    BULK_STRING('$'),
    ARRAY('*');

    char typePrefix;

    RespType(char typePrefix) {
        this.typePrefix = typePrefix;
    }

    public static RespType of(char typePrefix) {
        for (RespType type : RespType.values()) {
            if (type.typePrefix == typePrefix) {
                return type;
            }
        }
        return null;
    }
}
