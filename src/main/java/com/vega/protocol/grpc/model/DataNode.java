package com.vega.protocol.grpc.model;

import com.vega.protocol.grpc.constant.VegaEnvironment;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class DataNode {
    private Integer id;
    private String grpcUrl;
    private String tmUrl;
    private Boolean healthy;
    private VegaEnvironment environment;
}