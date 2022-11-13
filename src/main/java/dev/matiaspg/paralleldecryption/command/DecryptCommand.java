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
import java.util.Arrays;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

@Slf4j
@ShellComponent
@RequiredArgsConstructor
public class DecryptCommand {
    private static final String DEFAULT_FILE = "csv/stories.csv";

    private final Decryptor decryptor;
    private final SimpleFileService simpleFileService;
    private final ChunkedFileService chunkedFileService;

    @ShellMethod
    public void decrypt(@ShellOption(value = {"--path", "-p"}, defaultValue = DEFAULT_FILE) String _path) throws IOException {
        byte[] whole = decryptWhole(_path);

        System.out.println();

        byte[] chunked = decryptChunked(_path);

        System.out.println();

        log.info("Results of \"whole\" and \"chunked\" decryption are the same: {}", Arrays.equals(whole, chunked));

        System.out.println();

        log.info("Checking that the decrypted result is the same as the original file");
        long decryptedChecksum = getChecksum(chunked);
        long originalChecksum = getChecksum(simpleFileService.read(Path.of(_path)));
        log.info("Decrypted result is the same as the original file: {}", decryptedChecksum == originalChecksum);
    }

    @ShellMethod
    public byte[] decryptWhole(@ShellOption(value = {"--path", "-p"}, defaultValue = DEFAULT_FILE) String _path) throws IOException {
        Path path = Path.of(_path.concat(".encrypted"));

        long start = System.currentTimeMillis();

        log.info("Getting file contents");
        byte[] content = simpleFileService.read(path);

        log.info("Decrypting file");
        byte[] decryptedContent = decryptor.decrypt(content);

        long end = System.currentTimeMillis();

        log.info("Reading and decrypting a whole file of {} bytes took {} ms", decryptedContent.length, end - start);

        return decryptedContent;
    }

    @ShellMethod
    public byte[] decryptChunked(@ShellOption(value = {"--path", "-p"}, defaultValue = DEFAULT_FILE) String _path) throws IOException {
        Path path = Path.of(_path.concat(".encrypted"));

        long start = System.currentTimeMillis();

        log.info("Getting file chunks and decrypting them");
        byte[] decryptedContent = chunkedFileService.read(path, decryptor::decrypt);

        long end = System.currentTimeMillis();

        log.info("Reading and decrypting in parallel a file of {} bytes took {} ms", decryptedContent.length, end - start);

        return decryptedContent;
    }

    private long getChecksum(byte[] bytes) {
        Checksum crc32 = new CRC32();
        crc32.update(bytes, 0, bytes.length);
        return crc32.getValue();
    }
}
