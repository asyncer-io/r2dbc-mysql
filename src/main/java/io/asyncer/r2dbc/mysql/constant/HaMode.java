package io.asyncer.r2dbc.mysql.constant;

import org.jetbrains.annotations.Nullable;

public enum HaMode {
    NONE(HostsCardinality.SINGLE),
    FALLBACK(HostsCardinality.MULTIPLE),
    LOADBALANCE(HostsCardinality.ONE_OR_MORE),
    ;

    private final HostsCardinality hostsCardinality;

    HaMode(HostsCardinality hostsCardinality) {
        this.hostsCardinality = hostsCardinality;
    }

    public static HaMode parse(@Nullable String haMode, int hostCardinality) {
        if (hostCardinality == 1) {
           if (null == haMode || NONE.name().equalsIgnoreCase(haMode)) {
               return NONE;
           }
        }
        if (hostCardinality > 1) {
            if (null == haMode || FALLBACK.name().equalsIgnoreCase(haMode)) {
                return FALLBACK;
            }
            if (LOADBALANCE.name().equalsIgnoreCase(haMode)) {
                return LOADBALANCE;
            }

        }
        throw new IllegalArgumentException("Invalid haMode: " + haMode + ", hostCardinality: " + hostCardinality);
    }

    public enum HostsCardinality {
        SINGLE,
        ONE_OR_MORE,
        MULTIPLE,
    }
}
