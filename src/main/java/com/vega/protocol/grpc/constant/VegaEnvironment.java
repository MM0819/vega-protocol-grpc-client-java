package com.vega.protocol.grpc.constant;

import lombok.Getter;

public enum VegaEnvironment {

    STAGNET("stagnet1"), FAIRGROUND("testnet"), MAINNET("mainnet");
    @Getter
    private final String prefix;

    VegaEnvironment(String prefix) {
        this.prefix = prefix;
    }
}