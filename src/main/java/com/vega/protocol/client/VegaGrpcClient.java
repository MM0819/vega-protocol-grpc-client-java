package com.vega.protocol.client;

import com.vega.protocol.observer.VegaStreamObserver;
import com.vega.protocol.utils.VegaAuthUtils;
import datanode.api.v2.TradingData;
import datanode.api.v2.TradingDataServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.crypto.params.Ed25519PrivateKeyParameters;
import org.bouncycastle.crypto.params.Ed25519PublicKeyParameters;
import vega.Assets;
import vega.Governance;
import vega.Markets;
import vega.Vega;
import vega.api.v1.Core;
import vega.api.v1.CoreServiceGrpc;
import vega.commands.v1.Commands;
import vega.commands.v1.TransactionOuterClass;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
public class VegaGrpcClient {

    private static final String DEFAULT_HOSTNAME = "api.n10.testnet.vega.xyz";
    private static final int DEFAULT_PORT = 3002;

    private final String privateKey;

    // TODO - allow user to pass Wallet object containing multiple keys
    public VegaGrpcClient(
            final String privateKey
    ) {
        this.privateKey = privateKey;
    }

    /**
     * Get the data node blocking client
     *
     * @return {@link TradingDataServiceGrpc.TradingDataServiceBlockingStub}
     */
    public TradingDataServiceGrpc.TradingDataServiceBlockingStub getClient() {
        return TradingDataServiceGrpc.newBlockingStub(getChannel());
    }

    /**
     * Get the data node non-blocking client (for streaming)
     *
     * @return {@link TradingDataServiceGrpc.TradingDataServiceStub}
     */
    public TradingDataServiceGrpc.TradingDataServiceStub getStreamingClient() {
        return TradingDataServiceGrpc.newStub(getChannel());
    }

    /**
     * Get the gRPC channel
     *
     * @return {@link ManagedChannel}
     */
    private ManagedChannel getChannel() {
        return ManagedChannelBuilder.forAddress(getHostname(), getPort()).usePlaintext().build();
    }

    /**
     * Get the hostname from environment
     *
     * @return $HOSTNAME or api.n10.testnet.vega.xyz
     */
    private String getHostname() {
        return System.getenv().getOrDefault("HOSTNAME", DEFAULT_HOSTNAME);
    }

    /**
     * Get the port from environment
     *
     * @return $PORT or 3007
     */
    private int getPort() {
        return Integer.parseInt(System.getenv().getOrDefault("PORT", String.valueOf(DEFAULT_PORT)));
    }

    /**
     * Get the core gRPC client (blocking)
     *
     * @return {@link CoreServiceGrpc.CoreServiceBlockingStub}
     */
    public CoreServiceGrpc.CoreServiceBlockingStub getCoreClient() {
        return CoreServiceGrpc.newBlockingStub(getChannel());
    }

    /**
     * Sign and send a transaction
     *
     * @param lastBlock the latest block {@link Core.LastBlockHeightResponse}
     * @param inputData the input data {@link TransactionOuterClass.InputData}
     */
    private void signAndSend(
            final Core.LastBlockHeightResponse lastBlock,
            final TransactionOuterClass.InputData inputData
    ) {
        try {
            String chainId = lastBlock.getChainId();
            int difficulty = lastBlock.getSpamPowDifficulty();
            String blockHash = lastBlock.getHash();
            String hashFunction = lastBlock.getSpamPowHashFunction();
            Ed25519PrivateKeyParameters privateKeyRebuild = new Ed25519PrivateKeyParameters(Hex.decodeHex(privateKey), 0);
            Ed25519PublicKeyParameters publicKeyRebuild = privateKeyRebuild.generatePublicKey();
            String publicKey = Hex.encodeHexString(publicKeyRebuild.getEncoded());
            var tx = VegaAuthUtils.buildTx(publicKey, privateKey,
                    chainId, difficulty, blockHash, hashFunction, inputData);
            Core.SubmitTransactionRequest submitTx = Core.SubmitTransactionRequest.newBuilder()
                    .setTx(tx).build();
            var response = getCoreClient().submitTransaction(submitTx);
            log.info(response.toString());
        } catch(Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * Vote on a proposal
     *
     * @param proposalId the proposal ID
     * @param value {@link vega.Governance.Vote.Value}
     */
    public void voteOnProposal(
            final String proposalId,
            final Governance.Vote.Value value
    ) {
        var voteSubmission = Commands.VoteSubmission.newBuilder()
                .setProposalId(proposalId)
                .setValue(value)
                .build();
        var lastBlock = getLastBlock();
        long nonce = Math.abs(new Random().nextLong());
        long height = lastBlock.getHeight();
        var inputData = TransactionOuterClass.InputData.newBuilder()
                .setNonce(nonce)
                .setBlockHeight(height)
                .setVoteSubmission(voteSubmission)
                .build();
        signAndSend(lastBlock, inputData);
    }

    /**
     * Submit an order
     *
     * @param price the order price
     * @param size the order size
     * @param side {@link vega.Vega.Side}
     * @param timeInForce {@link vega.Vega.Order.TimeInForce}
     * @param type {@link vega.Vega.Order.Type}
     * @param marketId the market ID
     */
    public void submitOrder(
            final String price,
            final long size,
            final Vega.Side side,
            final Vega.Order.TimeInForce timeInForce,
            final Vega.Order.Type type,
            final String marketId
    ) {
        var orderSubmission = Commands.OrderSubmission.newBuilder()
                .setPrice(price)
                .setSize(size)
                .setSide(side)
                .setTimeInForce(timeInForce)
                .setMarketId(marketId)
                .setType(type)
                .build();
        var lastBlock = getLastBlock();
        long nonce = Math.abs(new Random().nextLong());
        long height = lastBlock.getHeight();
        var inputData = TransactionOuterClass.InputData.newBuilder()
                .setNonce(nonce)
                .setBlockHeight(height)
                .setOrderSubmission(orderSubmission)
                .build();
        signAndSend(lastBlock, inputData);
    }

    /**
     * Get the last block
     *
     * @return {@link vega.api.v1.Core.LastBlockHeightResponse}
     */
    public Core.LastBlockHeightResponse getLastBlock() {
        var request = Core.LastBlockHeightRequest.newBuilder().build();
        return getCoreClient().lastBlockHeight(request);
    }

    /**
     * Get all markets
     *
     * @return {@link List<vega.Markets.Market>}
     */
    public List<Markets.Market> getMarkets() {
        var builder = TradingData.ListMarketsRequest.newBuilder();
        var request = builder.build();
        var markets = getClient().listMarkets(request).getMarkets().getEdgesList();
        return markets.stream().map(TradingData.MarketEdge::getNode).collect(Collectors.toList());
    }

    /**
     * Get all assets
     *
     * @return {@link List<vega.Assets.Asset>}
     */
    public List<Assets.Asset> getAssets() {
        var builder = TradingData.ListAssetsRequest.newBuilder();
        var request = builder.build();
        var assets = getClient().listAssets(request).getAssets().getEdgesList();
        return assets.stream().map(TradingData.AssetEdge::getNode).collect(Collectors.toList());
    }

    /**
     * Get all positions
     *
     * @param partyId optional party ID
     * @param marketId optional market ID
     *
     * @return {@link List<vega.Vega.Position>}
     */
    public List<Vega.Position> getPositions(
            final String partyId,
            final String marketId
    ) {
        var builder = TradingData.ListPositionsRequest.newBuilder();
        if(!StringUtils.isEmpty(partyId)) {
            builder = builder.setPartyId(partyId);
        }
        if(StringUtils.isEmpty(marketId)) {
            builder = builder.setMarketId(marketId);
        }
        var request = builder.build();
        var positions = getClient().listPositions(request).getPositions().getEdgesList();
        return positions.stream().map(TradingData.PositionEdge::getNode).collect(Collectors.toList());
    }

    /**
     * Get all accounts
     *
     * @param partyIds optional party IDs
     *
     * @return {@link List<TradingData.AccountBalance>}
     */
    public List<TradingData.AccountBalance> getAccounts(
            final List<String> partyIds
    ) {
        var builder = TradingData.ListAccountsRequest.newBuilder();
        var filter = TradingData.AccountFilter.newBuilder();
        if(!partyIds.isEmpty()) {
            builder.setFilter(filter.addAllPartyIds(partyIds).build());
        }
        var request = builder.build();
        var accounts = getClient().listAccounts(request).getAccounts().getEdgesList();
        return accounts.stream().map(TradingData.AccountEdge::getAccount).collect(Collectors.toList());
    }

    /**
     * Create a stream for positions
     *
     * @param callback callback function accepting {@link List<vega.Vega.Position>}
     */
    public void streamPositions(
            final Consumer<List<Vega.Position>> callback
    ) {
        streamPositions(null, callback);
    }

    /**
     * Create a stream for positions
     *
     * @param partyId optional party ID
     * @param callback callback function accepting {@link List<vega.Vega.Position>}
     */
    public void streamPositions(
            final String partyId,
            final Consumer<List<Vega.Position>> callback
    ) {
        var responseObserver = new VegaStreamObserver<TradingData.ObservePositionsResponse>() {
            @Override
            public void onNext(TradingData.ObservePositionsResponse response) {
                if(response.hasSnapshot()) {
                    callback.accept(response.getSnapshot().getPositionsList());
                } else {
                    callback.accept(response.getUpdates().getPositionsList());
                }
            }
        };
        var builder = TradingData.ObservePositionsRequest.newBuilder();
        if(!StringUtils.isEmpty(partyId)) {
            builder = builder.setPartyId(partyId);
        }
        var request = builder.build();
        var client = getStreamingClient();
        client.observePositions(request, responseObserver);
    }

    /**
     * Create a stream for trades
     *
     * @param callback callback function accepting {@link List<vega.Vega.Trade>}
     */
    public void streamTrades(
            final Consumer<List<Vega.Trade>> callback
    ) {
        streamTrades(null, callback);
    }

    /**
     * Create a stream for trades
     *
     * @param marketId optional market ID
     * @param callback callback function accepting {@link List<vega.Vega.Trade>}
     */
    public void streamTrades(
            final String marketId,
            final Consumer<List<Vega.Trade>> callback
    ) {
        var responseObserver = new VegaStreamObserver<TradingData.ObserveTradesResponse>() {
            @Override
            public void onNext(TradingData.ObserveTradesResponse response) {
                callback.accept(response.getTradesList());
            }
        };
        var builder = TradingData.ObserveTradesRequest.newBuilder();
        if(!StringUtils.isEmpty(marketId)) {
            builder = builder.setMarketId(marketId);
        }
        var request = builder.build();
        var client = getStreamingClient();
        client.observeTrades(request, responseObserver);
    }

    /**
     * Create a stream for accounts
     *
     * @param callback callback function accepting {@link List<datanode.api.v2.TradingData.AccountBalance>}
     */
    public void streamAccounts(
            final Consumer<List<TradingData.AccountBalance>> callback
    ) {
        streamAccounts(null, callback);
    }

    /**
     * Create a stream for accounts
     *
     * @param partyId optional party ID
     * @param callback callback function accepting {@link List<datanode.api.v2.TradingData.AccountBalance>}
     */
    public void streamAccounts(
            final String partyId,
            final Consumer<List<TradingData.AccountBalance>> callback
    ) {
        var responseObserver = new VegaStreamObserver<TradingData.ObserveAccountsResponse>() {
            @Override
            public void onNext(TradingData.ObserveAccountsResponse response) {
                if(response.hasSnapshot()) {
                    callback.accept(response.getSnapshot().getAccountsList());
                } else {
                    callback.accept(response.getUpdates().getAccountsList());
                }
            }
        };
        var builder = TradingData.ObserveAccountsRequest.newBuilder();
        if(!StringUtils.isEmpty(partyId)) {
            builder = builder.setPartyId(partyId);
        }
        var request = builder.build();
        var client = getStreamingClient();
        client.observeAccounts(request, responseObserver);
    }

    /**
     * Create a stream for orders
     *
     * @param callback callback function accepting {@link List<vega.Vega.Order>}
     */
    public void streamOrders(
            final Consumer<List<Vega.Order>> callback
    ) {
        streamOrders(null, callback);
    }

    /**
     * Create a stream for orders
     *
     * @param partyId optional party ID
     * @param callback callback function accepting {@link List<vega.Vega.Order>}
     */
    public void streamOrders(
            final String partyId,
            final Consumer<List<Vega.Order>> callback
    ) {
        var responseObserver = new VegaStreamObserver<TradingData.ObserveOrdersResponse>() {
            @Override
            public void onNext(TradingData.ObserveOrdersResponse response) {
                if(response.hasSnapshot()) {
                    callback.accept(response.getSnapshot().getOrdersList());
                } else {
                    callback.accept(response.getUpdates().getOrdersList());
                }
            }
        };
        var builder = TradingData.ObserveOrdersRequest.newBuilder();
        if(!StringUtils.isEmpty(partyId)) {
            builder = builder.setPartyId(partyId);
        }
        var request = builder.build();
        var client = getStreamingClient();
        client.observeOrders(request, responseObserver);
    }

    /**
     * Create a stream for market data
     *
     * @param callback callback function accepting {@link List<vega.Vega.MarketData>}
     */
    public void streamMarketData(
            final Consumer<List<Vega.MarketData>> callback
    ) {
        streamMarketData(Collections.emptyList(), callback);
    }

    /**
     * Create a stream for market data
     *
     * @param marketIds optional market IDs
     * @param callback callback function accepting {@link List<vega.Vega.MarketData>}
     */
    public void streamMarketData(
            final List<String> marketIds,
            final Consumer<List<Vega.MarketData>> callback
    ) {
        var responseObserver = new VegaStreamObserver<TradingData.ObserveMarketsDataResponse>() {
            @Override
            public void onNext(TradingData.ObserveMarketsDataResponse response) {
                callback.accept(response.getMarketDataList());
            }
        };
        var builder = TradingData.ObserveMarketsDataRequest.newBuilder();
        if(!marketIds.isEmpty()) {
            builder.addAllMarketIds(marketIds);
        }
        var request = builder.build();
        var client = getStreamingClient();
        client.observeMarketsData(request, responseObserver);
    }

    /**
     * Create a stream for liquidity commitments
     *
     * @param callback callback function accepting {@link List<vega.Vega.LiquidityProvision>}
     */
    public void streamLiquidityCommitments(
            final Consumer<List<Vega.LiquidityProvision>> callback
    ) {
        streamLiquidityCommitments(null, callback);
    }

    /**
     * Create a stream for liquidity commitments
     *
     * @param partyId optional party ID
     * @param callback callback function accepting {@link List<vega.Vega.LiquidityProvision>}
     */
    public void streamLiquidityCommitments(
            final String partyId,
            final Consumer<List<Vega.LiquidityProvision>> callback
    ) {
        var responseObserver = new VegaStreamObserver<TradingData.ObserveLiquidityProvisionsResponse>() {
            @Override
            public void onNext(TradingData.ObserveLiquidityProvisionsResponse response) {
                callback.accept(response.getLiquidityProvisionsList());
            }
        };
        var builder = TradingData.ObserveLiquidityProvisionsRequest.newBuilder();
        if(!StringUtils.isEmpty(partyId)) {
            builder = builder.setPartyId(partyId);
        }
        var request = builder.build();
        var client = getStreamingClient();
        client.observeLiquidityProvisions(request, responseObserver);
    }
}