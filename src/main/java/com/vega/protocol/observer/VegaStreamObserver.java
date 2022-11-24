package com.vega.protocol.observer;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class VegaStreamObserver<T> implements StreamObserver<T> {

    @Override
    public void onError(Throwable e) {
        log.error(e.getMessage(), e);
    }

    @Override
    public void onCompleted() {
        log.debug("Complete: {}", this.getClass().getSimpleName());
    }
}
