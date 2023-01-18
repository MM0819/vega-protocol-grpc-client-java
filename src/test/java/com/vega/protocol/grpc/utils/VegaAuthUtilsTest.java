package com.vega.protocol.grpc.utils;

import com.vega.protocol.grpc.error.ErrorCode;
import com.vega.protocol.grpc.exception.VegaGrpcClientException;
import com.vega.protocol.grpc.model.KeyPair;
import com.vega.protocol.grpc.model.ProofOfWork;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import vega.Governance;
import vega.commands.v1.Commands;
import vega.commands.v1.TransactionOuterClass;

@Slf4j
public class VegaAuthUtilsTest {

    private void buildTx(String powHashFunction) throws Exception {
        String publicKey = "090062f920485250e5edf498d3ae030a6ab64386a29ab8f874f96049eb493471";
        String privateKey = "59dfdcad33e8a1487a4ac4fa5ec53d903b28857d97db5cd30cc8adc0da119ba" +
                "e090062f920485250e5edf498d3ae030a6ab64386a29ab8f874f96049eb493471";
        String proposalId = "eb2d3902fdda9c3eb6e369f2235689b871c7322cf3ab284dde3e9dfc13863a17";
        String chainId = "RYbuR";
        long blockHeight = 100L;
        int difficulty = 3;
        String blockHash = "6N8aegE5lWCTAgJWbJGoMquxZhyONKJfgDhBSqUjm5ID74dxB2zaoYuoyyUBRLWN";
        var voteSubmission = Commands.VoteSubmission.newBuilder()
                .setProposalId(proposalId)
                .setValue(Governance.Vote.Value.VALUE_YES)
                .build();
        var inputData = TransactionOuterClass.InputData.newBuilder()
                .setNonce(1L)
                .setBlockHeight(blockHeight)
                .setVoteSubmission(voteSubmission)
                .build();
        var txId = "1";
        var nonce = VegaAuthUtils.pow(difficulty, blockHash, txId, powHashFunction);
        var pow = new ProofOfWork()
                .setUsed(false)
                .setDifficulty(difficulty)
                .setBlockHeight(blockHeight)
                .setBlockHash(blockHash)
                .setTxId(txId)
                .setNonce(nonce);
        KeyPair keyPair = new KeyPair().setPrivateKey(privateKey).setPublicKey(publicKey);
        String tx = Base64.encodeBase64String(VegaAuthUtils.buildTx(keyPair, chainId, pow, inputData).toByteArray());
        Assertions.assertNotNull(tx);
    }

    @Test
    public void testBuildTx() throws Exception {
        buildTx("sha3_24_rounds");
    }

    @Test
    public void testBuildTxWithInvalidPowFunction() throws Exception {
        try {
            buildTx("sha3_36_rounds");
        } catch(VegaGrpcClientException e) {
            Assertions.assertEquals(e.getMessage(), ErrorCode.UNSUPPORTED_HASH_FUNCTION);
        }
    }
}