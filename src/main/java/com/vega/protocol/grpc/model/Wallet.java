package com.vega.protocol.grpc.model;

import com.vega.protocol.grpc.utils.VegaAuthUtils;
import org.apache.commons.codec.DecoderException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Wallet {
    private final String mnemonic;

    private final List<KeyPair> derivedKeys = new ArrayList<>();
    private final List<KeyPair> importedKeys = new ArrayList<>();

    public Wallet(
            final String mnemonic
    ) {
        this.mnemonic = mnemonic;
    }

    public KeyPair get(
            final int index
    ) {
        if(derivedKeys.size() <= index) {
            // TODO - if the key doesn't exist, derive it and then store it in the array list
        }
        return derivedKeys.get(index);
    }

    public Optional<KeyPair> getWithPubKey(
            final String pubKey
    ) {
        List<KeyPair> allKeys = new ArrayList<>();
        allKeys.addAll(derivedKeys);
        allKeys.addAll(importedKeys);
        return allKeys.stream().filter(k -> k.getPublicKey().equals(pubKey)).findFirst();
    }

    public void importKey(
            final String privateKey
    ) throws DecoderException {
        String publicKey = VegaAuthUtils.getPublicKey(privateKey);
        importedKeys.add(new KeyPair().setPublicKey(publicKey).setPrivateKey(privateKey));
    }
}