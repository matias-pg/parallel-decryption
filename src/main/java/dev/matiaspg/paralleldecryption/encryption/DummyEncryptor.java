package dev.matiaspg.paralleldecryption.encryption;

import lombok.SneakyThrows;
import org.springframework.stereotype.Component;

import java.util.Base64;

@Component
public class DummyEncryptor implements Encryptor {
    private final static Base64.Encoder encoder = Base64.getEncoder();

    @Override
    public byte[] encrypt(byte[] unencrypted) {
        delayToSimulateSlowEncryptionAlgorithm(unencrypted.length);
        return encoder.encode(unencrypted);
    }

    @SneakyThrows({InterruptedException.class})
    private void delayToSimulateSlowEncryptionAlgorithm(int contentLength) {
        // Divide by a smaller number to make it slower
        // The times indicate how long it takes to encrypt a whole file of 554 MiB
        // - 100_000 -> 6.7 seconds
        // - 10_000 -> 59.0 seconds
        // - 1_000 -> 581.9 seconds (9m 41.9s)
        // - 222 -> 2,618.1 seconds (43m 38.1s)
        Thread.sleep(contentLength / 222);
    }
}
