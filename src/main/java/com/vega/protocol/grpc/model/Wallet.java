package com.vega.protocol.grpc.model;

import com.vega.protocol.grpc.utils.VegaAuthUtils;
import lombok.extern.slf4j.Slf4j;
import org.bitcoinj.crypto.DeterministicKey;
import org.bitcoinj.crypto.HDKeyDerivation;
import org.bitcoinj.wallet.DeterministicSeed;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class Wallet {
    private final String mnemonic;
    private final Map<Integer, KeyPair> derivedKeys = new ConcurrentHashMap<>();
    private final List<KeyPair> importedKeys = new ArrayList<>();

    public Wallet(
            final String mnemonic
    ) {
        this.mnemonic = mnemonic;
    }

    public Optional<KeyPair> get(
            final int index
    ) {
        try {
            if (derivedKeys.get(index) == null) {
                var seed = new DeterministicSeed(mnemonic, null, "", 0);
                if(seed.getSeedBytes() == null) return Optional.empty();
                DeterministicKey root = HDKeyDerivation.createMasterPrivateKey(seed.getSeedBytes());
                DeterministicKey key = HDKeyDerivation.deriveChildKey(root, index);
                KeyPair keyPair = new KeyPair()
                        .setPrivateKey(key.getPrivateKeyAsHex())
                        .setPublicKey(VegaAuthUtils.getPublicKey(key.getPrivateKeyAsHex()));
                derivedKeys.put(index, keyPair);
                return Optional.of(keyPair);
            }
            return Optional.of(derivedKeys.get(index));
        } catch(Exception e) {
            log.error(e.getMessage(), e);
        }
        return Optional.empty();
    }

    public Optional<KeyPair> getWithPubKey(
            final String pubKey
    ) {
        List<KeyPair> allKeys = new ArrayList<>();
        allKeys.addAll(derivedKeys.values());
        allKeys.addAll(importedKeys);
        return allKeys.stream().filter(k -> k.getPublicKey().equals(pubKey)).findFirst();
    }

    public void importKey(
            final String privateKey
    ) {
        String publicKey = VegaAuthUtils.getPublicKey(privateKey);
        importedKeys.add(new KeyPair().setPublicKey(publicKey).setPrivateKey(privateKey));
    }
}