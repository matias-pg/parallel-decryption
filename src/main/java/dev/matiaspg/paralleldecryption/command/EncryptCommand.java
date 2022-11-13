package dev.matiaspg.paralleldecryption.command;

import dev.matiaspg.paralleldecryption.encryption.Encryptor;
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
public class EncryptCommand {
    private static final String DEFAULT_FILE = "src/main/resources/stories.csv";

    private final Encryptor encryptor;
    private final SimpleFileService simpleFileService;
    private final ChunkedFileService chunkedFileService;

    @ShellMethod
    public void encrypt(@ShellOption(value = {"--path", "-p"}, defaultValue = DEFAULT_FILE) String _path) throws IOException {
        encryptWhole(_path);

        System.out.println();

        encryptChunked(_path);
    }

    @ShellMethod
    public void encryptWhole(@ShellOption(value = {"--path", "-p"}, defaultValue = DEFAULT_FILE) String _path) throws IOException {
        Path path = Path.of(_path);
        Path targetPath = Path.of(path.toString().concat(".encrypted"));

        log.info("Getting file contents");
        byte[] content = simpleFileService.read(path);

        long start = System.currentTimeMillis();

        log.info("Encrypting file");
        byte[] encryptedContent = encryptor.encrypt(content);

        log.info("Writing encrypted file");
        simpleFileService.write(targetPath, encryptedContent);

        long end = System.currentTimeMillis();

        log.info("Encrypting and writing a whole file of {} bytes took {} ms", content.length, end - start);
    }

    @ShellMethod
    public void encryptChunked(@ShellOption(value = {"--path", "-p"}, defaultValue = DEFAULT_FILE) String _path) throws IOException {
        Path path = Path.of(_path);
        Path targetPath = Path.of(path.toString().concat(".encrypted"));

        log.info("Getting file contents");
        byte[] content = simpleFileService.read(path);

        long start = System.currentTimeMillis();

        log.info("Encrypting & writing file");
        chunkedFileService.write(targetPath, content, encryptor::encrypt);

        long end = System.currentTimeMillis();

        log.info("Encrypting and writing in parallel all chunks of a file of {} bytes took {} ms", content.length, end - start);
    }
}
