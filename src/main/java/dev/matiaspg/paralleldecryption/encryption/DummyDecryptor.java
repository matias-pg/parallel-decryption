package dev.matiaspg.paralleldecryption.encryption;

import lombok.SneakyThrows;
import org.springframework.stereotype.Component;

import java.util.Base64;

@Component
public class DummyDecryptor implements Decryptor {
    private final static Base64.Decoder decoder = Base64.getDecoder();

    @Override
    public byte[] decrypt(byte[] encrypted) {
        delayToSimulateSlowDecryptionAlgorithm(encrypted.length);
        return decoder.decode(encrypted);
    }

    @SneakyThrows({InterruptedException.class})
    private void delayToSimulateSlowDecryptionAlgorithm(int contentLength) {
        // Divide by a smaller number to make it slower
        // The times indicate how long it takes to decrypt a whole file of 554 MiB
        // - 100_000 -> 8.7 seconds
        // - 10_000 -> 78.4 seconds (1m 18.4s)
        // - 1_000 -> 775.6 seconds (12m 55.6s)
        // - 222 -> 3,490.5 seconds (58m 10.5s)
        Thread.sleep(contentLength / 222);
    }
}
