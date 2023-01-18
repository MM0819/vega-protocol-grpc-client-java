package com.vega.protocol.grpc.observer;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
public class VegaStreamObserverTest {

    private VegaStreamObserver<Object> vegaStreamObserver;

    @BeforeEach
    public void setup() {
        vegaStreamObserver = new VegaStreamObserver<>() {
            @Override
            public void onNext(Object o) {
                log.info("next");
            }
        };
    }

    @Test
    public void testOnError() {
        vegaStreamObserver.onError(new Exception("error"));
    }

    @Test
    public void testOnCompleted() {
        vegaStreamObserver.onCompleted();
    }
}