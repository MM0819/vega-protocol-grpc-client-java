package com.vega.protocol.grpc.model;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class WalletTest {
    private Wallet wallet;
    @BeforeEach
    public void setup() {
        wallet = new Wallet("jacket whale enroll enforce armor prosper desk loyal actual joke twelve " +
                "mule shed history medal juice inner enjoy measure cram distance motion churn chuckle");
    }

    @Test
    public void testGetByIndex() {
        var kp = wallet.get(0);
        Assertions.assertTrue(kp.isPresent());
        Assertions.assertEquals(kp.get().getPublicKey(),
                "6441334cec4ef230b5dcb52eb1e232ea831ab24551a8f8c688c3c155bed59fdd");
        Assertions.assertEquals(kp.get().getPrivateKey(),
                "52e671d552c9592dc752963c0e3f25b71d75eaeb704e8cfbd9b0b1832fd65144");
        kp = wallet.get(1);
        Assertions.assertTrue(kp.isPresent());
        Assertions.assertEquals(kp.get().getPublicKey(),
                "5b95a100857af002ff66649ad8706f5f769b724c01feefcd3bd4ba97b8e8701d");
        Assertions.assertEquals(kp.get().getPrivateKey(),
                "6d0bc2571bc372464867b4b46b23ed93042253526cc1e0c5dac2df97f065c902");
    }
}