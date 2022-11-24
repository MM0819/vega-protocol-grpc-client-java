package com.vega.protocol.client;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import vega.Markets;
import vega.Vega;

import java.util.List;

public class VegaGrpcClientTest {

    private final static String PRIVATE_KEY = "59dfdcad33e8a1487a4ac4fa5ec53d903b28857d97db5cd30cc8adc0da119bae090062f920485250e5edf498d3ae030a6ab64386a29ab8f874f96049eb493471";
    private final static String PUBLIC_KEY = "090062f920485250e5edf498d3ae030a6ab64386a29ab8f874f96049eb493471";

    private VegaGrpcClient vegaGrpcClient;

    @BeforeEach
    public void setup() {
        vegaGrpcClient = new VegaGrpcClient(PRIVATE_KEY, PUBLIC_KEY);
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

    @Test
    public void testSubmitOrder() {
        String marketId = "12345";
        vegaGrpcClient.submitOrder("1000", 100L, Vega.Side.SIDE_BUY, Vega
                .Order.TimeInForce.TIME_IN_FORCE_GTC, Vega.Order.Type.TYPE_LIMIT, marketId);
    }
}