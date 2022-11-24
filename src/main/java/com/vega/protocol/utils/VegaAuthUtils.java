package com.vega.protocol.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.bouncycastle.crypto.CryptoException;
import org.bouncycastle.crypto.Signer;
import org.bouncycastle.crypto.params.Ed25519PrivateKeyParameters;
import org.bouncycastle.crypto.signers.Ed25519Signer;
import vega.commands.v1.SignatureOuterClass;
import vega.commands.v1.TransactionOuterClass;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

@Slf4j
public class VegaAuthUtils {

    /**
     * Build and sign transaction
     *
     * @param publicKey the signing public key
     * @param privateKey the signing private key
     * @param chainId the chain ID
     * @param difficulty proof-of-work difficulty
     * @param blockHash recent block hash
     * @param powHashFunction proof-of-work hash function
     * @param inputData {@link vega.commands.v1.TransactionOuterClass.InputData}
     *
     * @return {@link TransactionOuterClass.Transaction}
     *
     * @throws Exception thrown if exception occurs while building and signing tx
     */
    public static TransactionOuterClass.Transaction buildTx(
            final String publicKey,
            final String privateKey,
            final String chainId,
            final int difficulty,
            final String blockHash,
            final String powHashFunction,
            final TransactionOuterClass.InputData inputData
    ) throws Exception {
        String txId = UUID.randomUUID().toString();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );
        outputStream.write(chainId.getBytes(StandardCharsets.UTF_8));
        outputStream.write("\u0000".getBytes(StandardCharsets.UTF_8));
        outputStream.write(inputData.toByteArray());
        byte[] inputDataPacked = outputStream.toByteArray();
        String hexSig = sign(privateKey, inputDataPacked);
        SignatureOuterClass.Signature signature = SignatureOuterClass.Signature.newBuilder()
                .setVersion(1)
                .setAlgo("vega/ed25519")
                .setValue(hexSig)
                .build();
        long nonce = pow(difficulty, blockHash, txId, powHashFunction);
        TransactionOuterClass.ProofOfWork proofOfWork = TransactionOuterClass.ProofOfWork.newBuilder()
                .setTid(txId)
                .setNonce(nonce).build();
        return TransactionOuterClass.Transaction.newBuilder()
                .setVersion(TransactionOuterClass.TxVersion.TX_VERSION_V3)
                .setSignature(signature)
                .setPubKey(publicKey)
                .setPow(proofOfWork)
                .setInputData(inputData.toByteString())
                .build();
    }

    /**
     * Execute proof-of-work
     *
     * @param difficulty target difficulty
     * @param blockHash previous block hash
     * @param txId the transaction ID
     * @param powHashFunction required hash function
     *
     * @return the valid nonce
     *
     * @throws Exception thrown if error occurs
     */
    private static long pow(
            final long difficulty,
            final String blockHash,
            final String txId,
            final String powHashFunction) throws Exception {
        long nonce = 0;
        byte[] hash;
        if(!powHashFunction.equals("sha3_24_rounds")) throw new Exception("Unsupported hash function !!");
        while (true) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );
            outputStream.write("Vega_SPAM_PoW".getBytes(StandardCharsets.UTF_8));
            outputStream.write(blockHash.getBytes(StandardCharsets.UTF_8));
            outputStream.write(txId.getBytes(StandardCharsets.UTF_8));
            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putLong(nonce);
            outputStream.write(buffer.array());
            byte[] dataPacked = outputStream.toByteArray();
            hash = sha3(dataPacked);
            int leadingZeroes = countZeroes(hash);
            if(leadingZeroes >= difficulty) {
                break;
            }
            nonce++;
        }
        return nonce;
    }

    /**
     * Count the leading zero bits in a number
     *
     * @param num the number
     *
     * @return number of leading zeroes
     */
    private static int lz(int num) {
        if(num == 0) return 8;
        int lz = 0;
        while ((num & (1 << 7)) == 0) {
            num = (num << 1);
            lz++;
        }
        return lz;
    }

    /**
     * Count the number of leading zero bits in a hash
     *
     * @param hash the hash as a byte array
     *
     * @return number of leading zero bits
     */
    private static int countZeroes(byte[] hash) {
        int zeroes = 0;
        for(byte b : hash) {
            int lz = lz(b);
            zeroes += lz;
            if(lz < 8) {
                break;
            }
        }
        return zeroes;
    }

    /**
     * Sign a message using ed25519 key
     *
     * @param key private key
     * @param msg the message as bytes
     *
     * @return the signature as hex string
     *
     * @throws DecoderException hex decoding error
     * @throws NoSuchAlgorithmException invalid hashing algorithm
     * @throws CryptoException error during hashing / signing
     */
    private static String sign(String key, byte[] msg) throws
            DecoderException, NoSuchAlgorithmException, CryptoException {
        Signer signer = new Ed25519Signer();
        signer.init(true, new Ed25519PrivateKeyParameters(Hex.decodeHex(key), 0));
        msg = sha3(msg);
        log.info("Hash before signing = {}", Hex.encodeHexString(msg));
        signer.update(msg, 0, msg.length);
        byte[] signature = signer.generateSignature();
        return Hex.encodeHexString(signature);
    }

    /**
     * Execute SHA3-256 hash of raw bytes
     *
     * @param data the input data
     *
     * @return the output bytes
     *
     * @throws NoSuchAlgorithmException invalid hashing algorithm
     */
    public static byte[] sha3(byte[] data) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA3-256");
        return digest.digest(data);
    }
}