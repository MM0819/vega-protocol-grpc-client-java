package com.vega.protocol.grpc.model;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class KeyPair {
    private String privateKey;
    private String publicKey;
}