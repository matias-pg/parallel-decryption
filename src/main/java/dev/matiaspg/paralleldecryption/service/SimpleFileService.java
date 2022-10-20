package dev.matiaspg.paralleldecryption.service;

import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@Service
public class SimpleFileService implements FileService {
    @Override
    public byte[] read(Path path) throws IOException {
        return Files.readAllBytes(path);
    }

    @Override
    public void write(Path path, byte[] content) throws IOException {
        Files.write(path, content);
    }
}
