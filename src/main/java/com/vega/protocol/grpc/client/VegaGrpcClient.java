package com.vega.protocol.grpc.client;

import com.vega.protocol.grpc.constant.VegaEnvironment;
import com.vega.protocol.grpc.error.ErrorCode;
import com.vega.protocol.grpc.exception.VegaGrpcClientException;
import com.vega.protocol.grpc.model.*;
import com.vega.protocol.grpc.observer.VegaStreamObserver;
import com.vega.protocol.grpc.utils.VegaAuthUtils;
import datanode.api.v2.TradingData;
import datanode.api.v2.TradingDataServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import vega.Assets;
import vega.Governance;
import vega.Markets;
import vega.Vega;
import vega.api.v1.Core;
import vega.api.v1.CoreServiceGrpc;
import vega.commands.v1.Commands;
import vega.commands.v1.TransactionOuterClass;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
public class VegaGrpcClient {
    private static final int DEFAULT_CORE_PORT = 3002;
    private static final int DEFAULT_DN_PORT = 3007;
    private static final int[] DEFAULT_STAGNET_NODES = new int[]{5, 6};
    private static final int[] DEFAULT_FAIRGROUND_NODES = new int[]{6, 7, 8, 9, 10, 11, 12};
    @Getter
    private final Wallet wallet;
    @Getter
    private final VegaEnvironment environment;
    private ManagedChannel channel = null;
    private ManagedChannel coreChannel = null;
    @Getter
    private Integer nodeId = 0;
    private final Map<Long, List<ProofOfWork>> powByBlock = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
    private static final List<DataNode> dataNodes = Collections.synchronizedList(new ArrayList<>());

    // TODO - we should have constructors that allow the user to override these defaults

    static {
        Arrays.stream(DEFAULT_STAGNET_NODES).forEach(id -> dataNodes.add(dataNode(id, VegaEnvironment.STAGNET)));
        Arrays.stream(DEFAULT_FAIRGROUND_NODES).forEach(id -> dataNodes.add(dataNode(id, VegaEnvironment.FAIRGROUND)));
    }

    /**
     * Create an instance of the {@link VegaGrpcClient}
     *
     * @param wallet {@link Wallet}
     */
    public VegaGrpcClient(
            final Wallet wallet,
            final VegaEnvironment environment
    ) {
        this.wallet = wallet;
        this.environment = environment;
        updateHealthStatus();
        scheduler.scheduleAtFixedRate(this::computeProofOfWork, 0, 1, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(this::updateHealthStatus, 0, 10, TimeUnit.SECONDS);
    }

    /**
     * Get the sync info from the Tendermint API
     *
     * @param tmUrl the Tendermint API URL
     *
     * @return {@link SyncInfo}
     */
    private static SyncInfo getSyncInfo(
            final String tmUrl
    ) {
        var client = HttpClient.newBuilder().build();
        var request = HttpRequest.newBuilder().GET().uri(URI.create(String.format("%s/status", tmUrl))).build();
        try {
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if(response.statusCode() != 200) {
                return new SyncInfo().setTimestamp(0L).setReplaying(true).setBlockHeight(0L);
            }
            var json = new JSONObject(response.body());
            var syncInfo = json.getJSONObject("result").getJSONObject("sync_info");
            var ts = syncInfo.getString("latest_block_time").split("\\.")[0];
            return new SyncInfo()
                    .setTimestamp(LocalDateTime.parse(ts).toEpochSecond(ZoneOffset.UTC))
                    .setReplaying(syncInfo.getBoolean("catching_up"))
                    .setBlockHeight(syncInfo.getLong("latest_block_height"));
        } catch(Exception e) {
            log.warn("Unhealthy data node {}: {}", tmUrl, e.getMessage());
        }
        return new SyncInfo().setTimestamp(0L).setReplaying(true).setBlockHeight(0L);
    }

    /**
     * Check if a data node is healthy by getting the Vega time from it and comparing to the current timestamp,
     * it will be considered unhealthy if data node is more than 10 seconds behind current time
     *
     * @param grpcUrl the grpc URL
     * @param tmUrl the Tendermint URL
     *
     * @return true / false
     */
    private static boolean isDataNodeHealthy(
            final String grpcUrl,
            final String tmUrl
    ) {
        try {
            var coreChannel = ManagedChannelBuilder.forAddress(grpcUrl, DEFAULT_CORE_PORT).usePlaintext().build();
            var dnChannel = ManagedChannelBuilder.forAddress(grpcUrl, DEFAULT_DN_PORT).usePlaintext().build();
            var coreStub = CoreServiceGrpc.newBlockingStub(coreChannel);
            var dnStub = TradingDataServiceGrpc.newBlockingStub(dnChannel);
            var dnTimeRequest = TradingData.GetVegaTimeRequest.newBuilder().build();
            var timeRequest = Core.GetVegaTimeRequest.newBuilder().build();
            var dnTimeResponse = dnStub.getVegaTime(dnTimeRequest);
            var timeResponse = coreStub.getVegaTime(timeRequest);
            var blockHeightRequest = Core.LastBlockHeightRequest.newBuilder().build();
            var blockHeightResponse = coreStub.lastBlockHeight(blockHeightRequest);
            var tsCore = timeResponse.getTimestamp() / 1000000000;
            var tsDN = dnTimeResponse.getTimestamp() / 1000000000;
            var now = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
            var timeDiffCore = Math.abs(now - tsCore);
            var timeDiffDN = Math.abs(now - tsDN);
            var syncInfo = getSyncInfo(tmUrl);
            coreChannel.shutdownNow();
            dnChannel.shutdownNow();
            var heightDiff = Math.abs(syncInfo.getBlockHeight() - blockHeightResponse.getHeight());
            var isHealthy = timeDiffCore <= 10 && timeDiffDN <= 10 && !syncInfo.getReplaying() && heightDiff <= 2;
            if(!isHealthy) {
                log.warn("Unhealthy data node {}: Time diff (core) = {}; Time diff (DN) = {}; replaying = {}; " +
                        "height diff = {}", tmUrl, timeDiffCore, timeDiffDN, syncInfo.getReplaying(), heightDiff);
            }
            return isHealthy;
        } catch(Exception e) {
            log.warn("Unhealthy data node {}: {}", tmUrl, e.getMessage());
            return false;
        }
    }

    /**
     * Build the data node object
     *
     * @param id data node ID
     * @param environment {@link VegaEnvironment}
     *
     * @return {@link DataNode}
     */
    private static DataNode dataNode(
            final int id,
            final VegaEnvironment environment
    ) {
        String grpcUrl = String.format("n%02d.%s.vega.xyz", id, environment.getPrefix());
        String tmUrl = String.format("https://tm.n%02d.%s.vega.xyz", id, environment.getPrefix());
        return new DataNode()
                .setId(id)
                .setTmUrl(tmUrl)
                .setGrpcUrl(grpcUrl)
                .setEnvironment(environment)
                .setHealthy(false);
    }

    /**
     * Shutdown channels if they exist
     */
    private void shutdownChannels() {
        try {
            if (channel != null) {
                channel.shutdownNow();
                channel.awaitTermination(10L, TimeUnit.SECONDS);
                channel = null;
            }
            if (coreChannel != null) {
                coreChannel.shutdownNow();
                coreChannel.awaitTermination(10L, TimeUnit.SECONDS);
                coreChannel = null;
            }
            nodeId = 0;
        } catch(InterruptedException e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * Initialize channels
     *
     * @param dataNode {@link DataNode}
     */
    private void initializeChannels(
            final DataNode dataNode
    ) {
        shutdownChannels();
        nodeId = dataNode.getId();
        channel = ManagedChannelBuilder.forAddress(dataNode.getGrpcUrl(), DEFAULT_DN_PORT).usePlaintext().build();
        coreChannel = ManagedChannelBuilder.forAddress(dataNode.getGrpcUrl(), DEFAULT_CORE_PORT).usePlaintext().build();
    }

    /**
     * Check if selected data node is healthy
     *
     * @return true / false
     */
    private boolean isSelectedNodeHealthy() {
        if(nodeId > 0) {
            return dataNodes.stream().anyMatch(dn -> dn.getId().equals(nodeId) &&
                    dn.getEnvironment().equals(environment) &&
                    dn.getHealthy());
        }
        return false;
    }

    /**
     * Periodically check if the date node is healthy
     */
    private void updateHealthStatus() {
        dataNodes.forEach(dn -> {
            if(dn.getEnvironment().equals(environment)) {
                var healthy = isDataNodeHealthy(dn.getGrpcUrl(), dn.getTmUrl());
                dn.setHealthy(healthy);
            }
        });
        if(nodeId == 0 || !isSelectedNodeHealthy()) {
            Optional<DataNode> dn = getHealthyDataNode();
            if(dn.isPresent()) {
                initializeChannels(dn.get());
            } else {
                shutdownChannels();
            }
        }
    }

    /**
     * Get a random healthy data node
     *
     * @return {@link DataNode}
     */
    public Optional<DataNode> getHealthyDataNode() {
        var nodes = dataNodes.stream().filter(dn -> dn.getHealthy() &&
                dn.getEnvironment().equals(environment)).collect(Collectors.toList());
        Collections.shuffle(nodes);
        return nodes.stream().findFirst();
    }

    /**
     * Get network parameter by key if it exists
     *
     * @param key the parameter key
     *
     * @return {@link Optional<Vega.NetworkParameter>}
     */
    public Optional<Vega.NetworkParameter> getNetworkParameterByKey(final String key) {
        return this.getNetworkParameters().stream().filter(p -> p.getKey().equals(key)).findFirst();
    }

    /**
     * Compute and store new proof-of-work
     */
    private void computeProofOfWork() {
        var lastBlock = getLastBlock();
        powByBlock.computeIfAbsent(lastBlock.getHeight(), k -> new ArrayList<>());
        var numberOfTxPerBlock = this.getNetworkParameterByKey("spam.pow.numberOfTxPerBlock");
        var numberOfPastBlocks = this.getNetworkParameterByKey("spam.pow.numberOfPastBlocks");
        if (powByBlock.get(lastBlock.getHeight()).size() == 0 &&
                numberOfTxPerBlock.isPresent() &&
                numberOfPastBlocks.isPresent()) {
            var txPerBlock = Integer.parseInt(numberOfTxPerBlock.get().getValue());
            var pastBlocks = Integer.parseInt(numberOfPastBlocks.get().getValue());
            int total = 20;
            for (int i = 1; i <= total; i++) {
                var difficulty = lastBlock.getSpamPowDifficulty();
                var extraZeroes = (int) Math.floor(((double) i) / ((double) txPerBlock));
                difficulty = difficulty + extraZeroes;
                try {
                    var txId = UUID.randomUUID().toString();
                    var nonce = VegaAuthUtils.pow(difficulty, lastBlock.getHash(),
                            txId, lastBlock.getSpamPowHashFunction());
                    var pow = new ProofOfWork()
                            .setUsed(false)
                            .setDifficulty(difficulty)
                            .setBlockHeight(lastBlock.getHeight())
                            .setBlockHash(lastBlock.getHash())
                            .setNonce(nonce)
                            .setTxId(txId);
                    powByBlock.get(lastBlock.getHeight()).add(pow);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
            var oldestBlock = lastBlock.getHeight() - Math.round(0.8 * pastBlocks);
            AtomicInteger i = new AtomicInteger();
            powByBlock.keySet().stream().filter(k -> k <= oldestBlock).forEach(height -> {
                powByBlock.remove(height);
                i.getAndIncrement();
            });
        }
    }

    /**
     * Taint a proof of work, so it won't be used twice
     *
     * @param blockHeight the block height
     * @param txId the unique transaction ID
     */
    private void taintProofOfWork(long blockHeight, final String txId) {
        List<ProofOfWork> powList = powByBlock.get(blockHeight);
        for(ProofOfWork pow : powList) {
            if(pow.getTxId().equals(txId)) {
                pow.setUsed(true);
            }
        }
    }

    /**
     * Get a new proof of work from the backlog
     *
     * @return {@link ProofOfWork}
     */
    private ProofOfWork getProofOfWork() {
        List<ProofOfWork> powList = powByBlock.values()
                .stream()
                .flatMap(Collection::stream)
                .filter(p -> !p.getUsed())
                .sorted(Comparator.comparing(ProofOfWork::getBlockHeight).thenComparing(ProofOfWork::getDifficulty))
                .toList();
        if(powList.size() == 0) {
            throw new VegaGrpcClientException(ErrorCode.WAITING_FOR_POW);
        }
        var pow = powList.get(0);
        taintProofOfWork(pow.getBlockHeight(), pow.getTxId());
        return pow;
    }

    /**
     * Get the data node blocking client
     *
     * @return {@link TradingDataServiceGrpc.TradingDataServiceBlockingStub}
     */
    public TradingDataServiceGrpc.TradingDataServiceBlockingStub getClient() {
        return TradingDataServiceGrpc.newBlockingStub(channel);
    }

    /**
     * Get the data node non-blocking client (for streaming)
     *
     * @return {@link Optional<TradingDataServiceGrpc.TradingDataServiceStub>}
     */
    public TradingDataServiceGrpc.TradingDataServiceStub getStreamingClient() {
        return TradingDataServiceGrpc.newStub(channel);
    }

    /**
     * Get the core gRPC client (blocking)
     *
     * @return {@link CoreServiceGrpc.CoreServiceBlockingStub}
     */
    public CoreServiceGrpc.CoreServiceBlockingStub getCoreClient() {
        return CoreServiceGrpc.newBlockingStub(coreChannel);
    }

    /**
     * Sign and send a transaction
     *
     * @param inputData the input data {@link TransactionOuterClass.InputData}
     * @param pow {@link ProofOfWork}
     * @param publicKey the signing key
     *
     * @return {@link Optional<Core.SubmitTransactionResponse>}
     */
    private Optional<Core.SubmitTransactionResponse> signAndSend(
            final TransactionOuterClass.InputData inputData,
            final ProofOfWork pow,
            final String publicKey
    ) {
        try {
            String chainId = getLastBlock().getChainId();
            KeyPair keyPair = wallet.getWithPubKey(publicKey)
                    .orElseThrow(() -> new VegaGrpcClientException(ErrorCode.PUB_KEY_NOT_FOUND));
            var tx = VegaAuthUtils.buildTx(keyPair, chainId, pow, inputData);
            Core.SubmitTransactionRequest submitTx = Core.SubmitTransactionRequest.newBuilder()
                    .setTx(tx).build();
            var response = getCoreClient().submitTransaction(submitTx);
            if (!response.getSuccess()) {
                log.error("Code = {}; Data = {}", response.getCode(), response.getData());
            }
            return Optional.of(response);
        } catch(Exception e) {
            log.error(e.getMessage(), e);
        }
        return Optional.empty();
    }

    private TransactionOuterClass.InputData.Builder getInputDataBuilder(
            final ProofOfWork pow
    ) {
        return TransactionOuterClass.InputData.newBuilder()
                .setNonce(Math.abs(new Random().nextLong()))
                .setBlockHeight(pow.getBlockHeight());
    }

    /**
     * Vote on a proposal
     *
     * @param proposalId the proposal ID
     * @param value {@link vega.Governance.Vote.Value}
     * @param publicKey the signing key
     *
     * @return {@link Optional<Core.SubmitTransactionResponse>}
     */
    public Optional<Core.SubmitTransactionResponse> voteOnProposal(
            final String proposalId,
            final Governance.Vote.Value value,
            final String publicKey
    ) {
        var voteSubmission = Commands.VoteSubmission.newBuilder()
                .setProposalId(proposalId)
                .setValue(value)
                .build();
        var pow = getProofOfWork();
        var inputData = getInputDataBuilder(pow)
                .setVoteSubmission(voteSubmission)
                .build();
        return signAndSend(inputData, pow, publicKey);
    }

    /**
     * Cancel an order
     *
     * @param orderId the order ID
     * @param marketId the market ID
     * @param publicKey the signing key
     *
     * @return {@link Optional<Core.SubmitTransactionResponse>}
     */
    public Optional<Core.SubmitTransactionResponse> cancelOrder(
            final String orderId,
            final String marketId,
            final String publicKey
    ) {
        var orderCancellation = Commands.OrderCancellation.newBuilder()
                .setOrderId(orderId)
                .setMarketId(marketId)
                .build();
        var pow = getProofOfWork();
        var inputData = getInputDataBuilder(pow)
                .setOrderCancellation(orderCancellation)
                .build();
        return signAndSend(inputData, pow, publicKey);
    }

    /**
     * Amend an order
     *
     * @param orderId the order ID
     * @param sizeDelta the change in size
     * @param price the price
     * @param marketId the market ID
     * @param publicKey the signing key
     *
     * @return {@link Optional<Core.SubmitTransactionResponse>}
     */
    public Optional<Core.SubmitTransactionResponse> amendOrder(
            final String orderId,
            final long sizeDelta,
            final String price,
            final String marketId,
            final String publicKey
    ) {
        var orderAmendment = Commands.OrderAmendment.newBuilder()
                .setOrderId(orderId)
                .setSizeDelta(sizeDelta)
                .setPrice(price)
                .setMarketId(marketId)
                .build();
        var pow = getProofOfWork();
        var inputData = getInputDataBuilder(pow)
                .setOrderAmendment(orderAmendment)
                .build();
        return signAndSend(inputData, pow, publicKey);
    }

    /**
     * Submit an order
     *
     * @param size the order size
     * @param side {@link vega.Vega.Side}
     * @param marketId the market ID
     * @param publicKey the signing key
     *
     * @return {@link Optional<Core.SubmitTransactionResponse>}
     */
    public Optional<Core.SubmitTransactionResponse> submitMarketOrder(
            final long size,
            final Vega.Side side,
            final String marketId,
            final String publicKey
    ) {
        return submitOrder("", size, side, Vega.Order.TimeInForce.TIME_IN_FORCE_IOC,
                Vega.Order.Type.TYPE_MARKET, marketId, publicKey);
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
     * @param publicKey the signing key
     *
     * @return {@link Optional<Core.SubmitTransactionResponse>}
     */
    public Optional<Core.SubmitTransactionResponse> submitOrder(
            final String price,
            final long size,
            final Vega.Side side,
            final Vega.Order.TimeInForce timeInForce,
            final Vega.Order.Type type,
            final String marketId,
            final String publicKey
    ) {
        var orderSubmission = Commands.OrderSubmission.newBuilder()
                .setPrice(price)
                .setSize(size)
                .setSide(side)
                .setTimeInForce(timeInForce)
                .setMarketId(marketId)
                .setType(type)
                .build();
        var pow = getProofOfWork();
        var inputData = getInputDataBuilder(pow)
                .setOrderSubmission(orderSubmission)
                .build();
        return signAndSend(inputData, pow, publicKey);
    }

    /**
     * Cancel a liquidity provision on a market
     *
     * @param marketId the market ID
     * @param publicKey the signing key
     *
     * @return {@link Optional<Core.SubmitTransactionResponse>}
     */
    public Optional<Core.SubmitTransactionResponse> cancelLiquidityProvision(
            final String marketId,
            final String publicKey
    ) {
        var liquidityProvisionCancellation = Commands.LiquidityProvisionCancellation.newBuilder()
                .setMarketId(marketId)
                .build();
        var pow = getProofOfWork();
        var inputData = getInputDataBuilder(pow)
                .setLiquidityProvisionCancellation(liquidityProvisionCancellation)
                .build();
        return signAndSend(inputData, pow, publicKey);
    }

    /**
     * Amend a liquidity provision on a market
     *
     * @param buys {@link List<vega.Vega.LiquidityOrder>}
     * @param sells {@link List<vega.Vega.LiquidityOrder>}
     * @param commitmentAmount the commitment amount
     * @param fee the proposed fee
     * @param marketId the market ID
     * @param publicKey the signing key
     *
     * @return {@link Optional<Core.SubmitTransactionResponse>}
     */
    public Optional<Core.SubmitTransactionResponse> amendLiquidityProvision(
            final List<Vega.LiquidityOrder> buys,
            final List<Vega.LiquidityOrder> sells,
            final String commitmentAmount,
            final String fee,
            final String marketId,
            final String publicKey
    ) {
        var liquidityProvisionAmendment = Commands.LiquidityProvisionAmendment.newBuilder()
                .addAllBuys(buys)
                .addAllSells(sells)
                .setCommitmentAmount(commitmentAmount)
                .setMarketId(marketId)
                .setFee(fee)
                .build();
        var pow = getProofOfWork();
        var inputData = getInputDataBuilder(pow)
                .setLiquidityProvisionAmendment(liquidityProvisionAmendment)
                .build();
        return signAndSend(inputData, pow, publicKey);
    }

    /**
     * Submit a liquidity provision to a market
     *
     * @param buys {@link List<vega.Vega.LiquidityOrder>}
     * @param sells {@link List<vega.Vega.LiquidityOrder>}
     * @param commitmentAmount the commitment amount
     * @param fee the proposed fee
     * @param marketId the market ID
     * @param publicKey the signing key
     *
     * @return {@link Optional<Core.SubmitTransactionResponse>}
     */
    public Optional<Core.SubmitTransactionResponse> submitLiquidityProvision(
            final List<Vega.LiquidityOrder> buys,
            final List<Vega.LiquidityOrder> sells,
            final String commitmentAmount,
            final String fee,
            final String marketId,
            final String publicKey
    ) {
        var liquidityProvisionSubmission = Commands.LiquidityProvisionSubmission.newBuilder()
                .addAllBuys(buys)
                .addAllSells(sells)
                .setCommitmentAmount(commitmentAmount)
                .setMarketId(marketId)
                .setFee(fee)
                .build();
        var pow = getProofOfWork();
        var inputData = getInputDataBuilder(pow)
                .setLiquidityProvisionSubmission(liquidityProvisionSubmission)
                .build();
        return signAndSend(inputData, pow, publicKey);
    }

    /**
     * Submit a batch market instruction
     *
     * @param amendments {@link List<vega.commands.v1.Commands.OrderAmendment>}
     * @param cancellations {@link List<vega.commands.v1.Commands.OrderCancellation>}
     * @param submissions {@link List<vega.commands.v1.Commands.OrderSubmission>}
     * @param publicKey the signing key
     *
     * @return {@link Optional<Core.SubmitTransactionResponse>}
     */
    public Optional<Core.SubmitTransactionResponse> batchMarketInstruction(
            final List<Commands.OrderAmendment> amendments,
            final List<Commands.OrderCancellation> cancellations,
            final List<Commands.OrderSubmission> submissions,
            final String publicKey
    ) {
        var batchMarketInstruction = Commands.BatchMarketInstructions.newBuilder()
                .addAllAmendments(amendments)
                .addAllSubmissions(submissions)
                .addAllCancellations(cancellations)
                .build();
        var pow = getProofOfWork();
        var inputData = getInputDataBuilder(pow)
                .setBatchMarketInstructions(batchMarketInstruction)
                .build();
        return signAndSend(inputData, pow, publicKey);
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
     * Get liquidity provisions
     *
     * @param partyId optional party ID
     * @param marketId optional market ID
     *
     * @return {@link List<vega.Vega.LiquidityProvision>}
     */
    public List<Vega.LiquidityProvision> getLiquidityProvisions(
            final String partyId,
            final String marketId
    ) {
        var builder = TradingData.ListLiquidityProvisionsRequest.newBuilder();
        if(!StringUtils.isEmpty(partyId)) {
            builder = builder.setPartyId(partyId);
        }
        if(StringUtils.isEmpty(marketId)) {
            builder = builder.setMarketId(marketId);
        }
        var request = builder.build();
        var liquidityProvisions = getClient().listLiquidityProvisions(request)
                .getLiquidityProvisions().getEdgesList();
        return liquidityProvisions.stream().map(TradingData.LiquidityProvisionsEdge::getNode)
                .collect(Collectors.toList());
    }

    /**
     * Get all network parameters
     *
     * @return {@link List<vega.Vega.NetworkParameter>}
     */
    public List<Vega.NetworkParameter> getNetworkParameters() {
        var request = TradingData.ListNetworkParametersRequest.newBuilder().build();
        var networkParameters = getClient().listNetworkParameters(request)
                .getNetworkParameters().getEdgesList();
        return networkParameters.stream().map(TradingData.NetworkParameterEdge::getNode).collect(Collectors.toList());
    }

    /**
     * Get orders
     *
     * @param partyId optional party ID
     * @param marketId optional market ID
     * @param liveOnly optional to filter active orders
     *
     * @return {@link List<vega.Vega.Order>}
     */
    public List<Vega.Order> getOrders(
            final String partyId,
            final String marketId,
            final Boolean liveOnly
    ) {
        var builder = TradingData.ListOrdersRequest.newBuilder();
        if(!StringUtils.isEmpty(partyId)) {
            builder = builder.setPartyId(partyId);
        }
        if(!StringUtils.isEmpty(marketId)) {
            builder = builder.setMarketId(marketId);
        }
        if(!ObjectUtils.isEmpty(liveOnly)) {
            builder.setLiveOnly(liveOnly);
        }
        var request = builder.build();
        var orders = getClient().listOrders(request).getOrders().getEdgesList();
        return orders.stream().map(TradingData.OrderEdge::getNode).collect(Collectors.toList());
    }

    /**
     * Get positions
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
        var resp = getClient().listPositions(request);
        var positions = resp.getPositions().getEdgesList();
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
    public void streamLiquidityProvisions(
            final Consumer<List<Vega.LiquidityProvision>> callback
    ) {
        streamLiquidityProvisions(null, callback);
    }

    /**
     * Create a stream for liquidity provisions
     *
     * @param partyId optional party ID
     * @param callback callback function accepting {@link List<vega.Vega.LiquidityProvision>}
     */
    public void streamLiquidityProvisions(
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