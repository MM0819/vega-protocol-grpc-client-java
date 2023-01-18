package com.vega.protocol.grpc.client;

import com.vega.protocol.grpc.model.Wallet;
import com.vega.protocol.grpc.utils.VegaAuthUtils;
import org.apache.commons.codec.DecoderException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import vega.Governance;
import vega.Markets;
import vega.Vega;
import vega.api.v1.Core;

import java.util.List;
import java.util.Optional;

public class VegaGrpcClientTest {

    private static final String MARKET_ID = "90e71c52b2f40db78efc24abe4217382993868cd24e45b3dd17147be4afaf884";
    private static final String PROPOSAL_ID = "7e2847d30ef2d4858f0f098c4251c789ad63ac9644e610ddb1cb014334a01ca6";
    private static final String PARTY_ID = "f8bc6b4508f3bd5a0207575836158c347f9984218d66e6d18cd316a92fb8d09a";

    private final static String PRIVATE_KEY = "e70da3716e54cfe4cbed58b584b85095bb4a8257a4b39ec91b491f29526430b6" +
            "053a10c3e8aa92bcfae80b61845a23a4dfc88d94a31570e3c494da9f43b64ca0";

    private static VegaGrpcClient vegaGrpcClient;

    @BeforeAll
    public static void setup() throws DecoderException {
        String mnemonic = "spawn item enter journey hill fringe collect type dress panel december solar receive " +
                "jazz pioneer account emerge drop squirrel spot owner seven earth brown";
        Wallet wallet = new Wallet(mnemonic);
        wallet.importKey(PRIVATE_KEY);
        vegaGrpcClient = new VegaGrpcClient(wallet, "n11.testnet.vega.xyz", 3007, 3002);
    }

    @Test
    public void testGetMarkets() {
        List<Markets.Market> markets = vegaGrpcClient.getMarkets();
        Assertions.assertTrue(markets.size() > 0);
    }

    @Test
    public void testStreamAccounts() {
        vegaGrpcClient.streamAccounts((accounts) -> Assertions.assertTrue(accounts.size() > 0));
    }

    @Test
    public void testStreamAccountsByPartyId() {
        vegaGrpcClient.streamAccounts(PARTY_ID, (accounts) -> Assertions.assertTrue(accounts.size() > 0));
    }

    @Test
    public void testStreamPositions() {
        vegaGrpcClient.streamPositions((positions) -> Assertions.assertTrue(positions.size() > 0));
    }

    @Test
    public void testStreamPositionsByPartyId() {
        vegaGrpcClient.streamPositions(PARTY_ID, (positions) -> Assertions.assertTrue(positions.size() > 0));
    }

    @Test
    public void testStreamOrders() {
        vegaGrpcClient.streamOrders((orders) -> Assertions.assertTrue(orders.size() > 0));
    }

    @Test
    public void testStreamOrdersByPartyId() {
        vegaGrpcClient.streamOrders(PARTY_ID, (orders) -> Assertions.assertTrue(orders.size() > 0));
    }

    @Test
    public void testStreamTrades() {
        vegaGrpcClient.streamTrades((trades) -> Assertions.assertTrue(trades.size() > 0));
    }

    @Test
    public void testStreamTradesByMarketId() {
        vegaGrpcClient.streamTrades(MARKET_ID, (trades) -> Assertions.assertTrue(trades.size() > 0));
    }

    @Test
    public void testStreamMarketData() {
        vegaGrpcClient.streamMarketData((data) -> Assertions.assertTrue(data.size() > 0));
    }

    @Test
    public void testStreamMarketDataByMarketId() {
        vegaGrpcClient.streamMarketData(List.of(MARKET_ID), (data) -> Assertions.assertTrue(data.size() > 0));
    }

    @Test
    public void testStreamLiquidityProvisions() {
        vegaGrpcClient.streamLiquidityProvisions((provisions) -> Assertions.assertTrue(provisions.size() > 0));
    }

    @Test
    public void testStreamLiquidityProvisionsByPartyId() {
        vegaGrpcClient.streamLiquidityProvisions(PARTY_ID, (provisions) -> Assertions.assertTrue(provisions.size() > 0));
    }

    @Test
    public void testSubmitOrder() throws Exception {
        Thread.sleep(2000L);
        String pubKey = VegaAuthUtils.getPublicKey(PRIVATE_KEY);
        Optional<Core.SubmitTransactionResponse> response = vegaGrpcClient.submitOrder(
                "1000", 100L, Vega.Side.SIDE_BUY, Vega.Order.TimeInForce.TIME_IN_FORCE_GTC,
                Vega.Order.Type.TYPE_LIMIT, MARKET_ID, pubKey
        );
        Assertions.assertTrue(response.isPresent());
        Assertions.assertTrue(response.get().getSuccess());
    }

    @Test
    public void testSubmitMarketOrder() throws Exception {
        Thread.sleep(2000L);
        String pubKey = VegaAuthUtils.getPublicKey(PRIVATE_KEY);
        Optional<Core.SubmitTransactionResponse> response = vegaGrpcClient.submitMarketOrder(
                100L, Vega.Side.SIDE_BUY, MARKET_ID, pubKey
        );
        Assertions.assertTrue(response.isPresent());
        Assertions.assertTrue(response.get().getSuccess());
    }

    @Test
    public void testVoteOnProposal() throws Exception {
        Thread.sleep(2000L);
        String pubKey = VegaAuthUtils.getPublicKey(PRIVATE_KEY);
        Optional<Core.SubmitTransactionResponse> response = vegaGrpcClient
                .voteOnProposal(PROPOSAL_ID, Governance.Vote.Value.VALUE_NO, pubKey);
        Assertions.assertTrue(response.isPresent());
        Assertions.assertFalse(response.get().getSuccess());
    }
}