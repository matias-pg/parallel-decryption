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
        // Divide by a smaller number to make it slower.
        // 10_000 -> 78 seconds on 550 MiB
        // 1_000 -> 780 seconds on 550 MiB (?)
        // 222 -> 56 minutes on 550 MiB
        Thread.sleep(contentLength / 222);
    }
}
