package dev.matiaspg.paralleldecryption.encryption;

import org.springframework.stereotype.Component;

import java.util.Base64;

@Component
public class DummyEncryptor implements Encryptor {
    private final static Base64.Encoder encoder = Base64.getEncoder();

    @Override
    public byte[] encrypt(byte[] unencrypted) {
        return encoder.encode(unencrypted);
    }
}
