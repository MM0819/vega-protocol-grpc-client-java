package com.vega.protocol.grpc.exception;

public class VegaGrpcClientException extends RuntimeException {
    public VegaGrpcClientException(String error) {
        super(error);
    }
}