package com.vega.protocol.grpc.model;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class ProofOfWork {
    private String blockHash;
    private Long blockHeight;
    private Integer difficulty;
    private Long nonce;
    private String txId;
    private Boolean used;
}
