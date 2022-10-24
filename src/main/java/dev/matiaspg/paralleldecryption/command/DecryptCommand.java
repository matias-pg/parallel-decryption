package dev.matiaspg.paralleldecryption.command;

import dev.matiaspg.paralleldecryption.encryption.Decryptor;
import dev.matiaspg.paralleldecryption.service.ChunkedFileService;
import dev.matiaspg.paralleldecryption.service.SimpleFileService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.io.IOException;
import java.nio.file.Path;

@Slf4j
@ShellComponent
@RequiredArgsConstructor
public class DecryptCommand {
    private static final String DEFAULT_FILE = "src/main/resources/stories.csv";

    private final Decryptor decryptor;
    private final SimpleFileService simpleFileService;
    private final ChunkedFileService chunkedFileService;

    @ShellMethod
    public void decrypt(@ShellOption(value = {"--path", "-p"}, defaultValue = DEFAULT_FILE) String _path) throws IOException {
        decryptWhole(_path);

        System.out.println();

        decryptChunked(_path);
    }

    @ShellMethod
    public void decryptWhole(@ShellOption(value = {"--path", "-p"}, defaultValue = DEFAULT_FILE) String _path) throws IOException {
        Path path = Path.of(_path.concat(".encrypted"));

        long start = System.currentTimeMillis();

        log.info("Getting file contents");
        byte[] content = simpleFileService.read(path);

        log.info("Decrypting file");
        byte[] decryptedContent = decryptor.decrypt(content);

        long end = System.currentTimeMillis();

        log.info("Reading and decrypting a whole file of {} bytes took {} ms", decryptedContent.length, end - start);
    }

    @ShellMethod
    public void decryptChunked(@ShellOption(value = {"--path", "-p"}, defaultValue = DEFAULT_FILE) String _path) throws IOException {
        Path path = Path.of(_path.concat(".encrypted.chunked"));

        long start = System.currentTimeMillis();

        log.info("Getting file chunks and decrypting them");
        byte[] decryptedContent = chunkedFileService.read(path, decryptor::decrypt);

        long end = System.currentTimeMillis();

        log.info("Reading and decrypting in parallel a file of {} bytes took {} ms", decryptedContent.length, end - start);
    }
}
