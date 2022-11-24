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

    @Test
    public void testStreamAccounts() {
        vegaGrpcClient.streamAccounts((accounts) -> Assertions.assertTrue(accounts.size() > 0));
    }

    @Test
    public void testStreamPositions() {
        vegaGrpcClient.streamPositions((positions) -> Assertions.assertTrue(positions.size() > 0));
    }

    @Test
    public void testStreamTrades() {
        vegaGrpcClient.streamTrades((trades) -> Assertions.assertTrue(trades.size() > 0));
    }
}