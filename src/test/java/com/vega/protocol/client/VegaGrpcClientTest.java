package com.vega.protocol.client;

import com.vega.protocol.grpc.client.VegaGrpcClient;
import com.vega.protocol.grpc.model.Wallet;
import com.vega.protocol.grpc.utils.VegaAuthUtils;
import org.apache.commons.codec.DecoderException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import vega.Governance;
import vega.Markets;
import vega.Vega;

import java.util.List;

public class VegaGrpcClientTest {

    private final static String PRIVATE_KEY = "e70da3716e54cfe4cbed58b584b85095bb4a8257a4b39ec91b491f29526430b6" +
            "053a10c3e8aa92bcfae80b61845a23a4dfc88d94a31570e3c494da9f43b64ca0";

    private VegaGrpcClient vegaGrpcClient;

    @BeforeEach
    public void setup() {
        String mnemonic = "spawn item enter journey hill fringe collect type dress panel december solar receive " +
                "jazz pioneer account emerge drop squirrel spot owner seven earth brown";
        Wallet wallet = new Wallet(mnemonic);
        vegaGrpcClient = new VegaGrpcClient(wallet);
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
    public void testSubmitOrder() throws DecoderException {
        String marketId = "90e71c52b2f40db78efc24abe4217382993868cd24e45b3dd17147be4afaf884";
        String pubKey = VegaAuthUtils.getPublicKey(PRIVATE_KEY);
        vegaGrpcClient.submitOrder("1000", 100L, Vega.Side.SIDE_BUY, Vega
                .Order.TimeInForce.TIME_IN_FORCE_GTC, Vega.Order.Type.TYPE_LIMIT, marketId, pubKey);
    }

    @Test
    public void testVoteOnProposal() throws DecoderException {
        String proposalId = "7e2847d30ef2d4858f0f098c4251c789ad63ac9644e610ddb1cb014334a01ca6";
        String pubKey = VegaAuthUtils.getPublicKey(PRIVATE_KEY);
        vegaGrpcClient.voteOnProposal(proposalId, Governance.Vote.Value.VALUE_NO, pubKey);
    }
}