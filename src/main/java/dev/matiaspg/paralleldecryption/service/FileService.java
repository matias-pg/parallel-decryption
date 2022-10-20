package dev.matiaspg.paralleldecryption.service;

import java.io.IOException;
import java.nio.file.Path;

public interface FileService {
    byte[] read(Path path) throws IOException;

    void write(Path path, byte[] content) throws IOException;
}
