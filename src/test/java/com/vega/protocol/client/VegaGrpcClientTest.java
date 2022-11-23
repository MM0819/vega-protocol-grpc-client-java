package com.vega.protocol.client;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import vega.Markets;

import java.util.List;

public class VegaGrpcClientTest {

    private VegaGrpcClient vegaGrpcClient;

    @BeforeEach
    public void setup() {
        vegaGrpcClient = new VegaGrpcClient();
    }

    @Test
    public void testGetMarkets() {
        List<Markets.Market> markets = vegaGrpcClient.getMarkets();
        Assertions.assertEquals(13, markets.size());
    }
}