package org.sredi.resp;

import lombok.Getter;
import lombok.Setter;

public abstract class RespValueBase implements RespValue {
    private final RespType type;

    @Setter
    @Getter
    private RespValueContext context;

    protected RespValueBase(RespType type) {
        this.type = type;
    }

    @Override
    public RespType getType() {
        return type;
    }

}
