package com.vega.protocol.grpc.error;

public class ErrorCode {
    private ErrorCode() {}
    public static final String PUB_KEY_NOT_FOUND = "Public key not found.";
    public static final String UNSUPPORTED_HASH_FUNCTION = "Unsupported hash function.";
    public static final String WAITING_FOR_POW = "Waiting for more proof of work";
}