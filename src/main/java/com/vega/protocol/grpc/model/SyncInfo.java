package com.vega.protocol.grpc.model;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class SyncInfo {
    private Long blockHeight;
    private Boolean replaying;
    private Long timestamp;
}