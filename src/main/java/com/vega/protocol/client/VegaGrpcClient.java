package com.vega.protocol.client;

import datanode.api.v2.TradingData;
import datanode.api.v2.TradingDataServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import vega.Assets;
import vega.Markets;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class VegaGrpcClient {

    private static final String DEFAULT_HOSTNAME = "api.n10.testnet.vega.xyz";
    private static final int DEFAULT_PORT = 3007;

    private TradingDataServiceGrpc.TradingDataServiceBlockingStub getClient() {
        return TradingDataServiceGrpc.newBlockingStub(getChannel());
    }

    private TradingDataServiceGrpc.TradingDataServiceStub getStreamingClient() {
        return TradingDataServiceGrpc.newStub(getChannel());
    }

    private ManagedChannel getChannel() {
        return ManagedChannelBuilder.forAddress(getHostname(), getPort()).usePlaintext().build();
    }

    private String getHostname() {
        return System.getenv().getOrDefault("HOSTNAME", DEFAULT_HOSTNAME);
    }

    private int getPort() {
        return Integer.parseInt(System.getenv().getOrDefault("PORT", String.valueOf(DEFAULT_PORT)));
    }

    /**
     * Get all markets
     *
     * @return {@link List<vega.Markets.Market>}
     */
    public List<Markets.Market> getMarkets() {
        TradingData.ListMarketsRequest request = TradingData.ListMarketsRequest.newBuilder().build();
        List<TradingData.MarketEdge> markets = getClient().listMarkets(request).getMarkets().getEdgesList();
        return markets.stream().map(TradingData.MarketEdge::getNode).collect(Collectors.toList());
    }

    /**
     * Get all assets
     *
     * @return {@link List<vega.Assets.Asset>}
     */
    public List<Assets.Asset> getAssets() {
        TradingData.ListAssetsRequest request = TradingData.ListAssetsRequest.newBuilder().build();
        List<TradingData.AssetEdge> assets = getClient().listAssets(request).getAssets().getEdgesList();
        return assets.stream().map(TradingData.AssetEdge::getNode).collect(Collectors.toList());
    }

    public void streamPositions() {
        var responseObserver = new StreamObserver<TradingData.ObservePositionsResponse>() {
            @Override
            public void onNext(TradingData.ObservePositionsResponse response) {
                log.info("Positions count = {}", response.getSnapshot().getPositionsList().size());
            }
            @Override
            public void onError(Throwable throwable) {
                log.error(throwable.getMessage());
            }
            @Override
            public void onCompleted() {
                log.info("Complete!");
            }
        };
        var request = TradingData.ObservePositionsRequest.newBuilder().build();
        var client = getStreamingClient();
        client.observePositions(request, responseObserver);
    }
}