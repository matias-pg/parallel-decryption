package dev.matiaspg.paralleldecryption.service;

import lombok.Cleanup;
import lombok.SneakyThrows;
import org.springframework.stereotype.Service;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.IntStream;

/**
 *
 */
@Service
public class ChunkedFileService implements FileService {
    /**
     * Size in bytes: 10 MiB.
     */
    private static final int CHUNK_SIZE = 1024 * 1024 * 10;

    private static final String CHUNK_NAME_TEMPLATE = "%s/chunk%s";
    private static final String TOTAL_CHUNKS_NAME_TEMPLATE = "%s/total_chunks";

    @Override
    public byte[] read(Path path) throws IOException {
        return read(path, Function.identity());
    }

    public byte[] read(Path path, Function<byte[], byte[]> chunkTransformer) throws IOException {
        // Get the total of chunks that have to be read
        int totalChunks = checkTotalChunks(path);

        // Read each chunk in parallel and transform its content
        List<byte[]> chunks = IntStream.range(0, totalChunks).parallel()
                .mapToObj(currentChunk -> doRead(path, currentChunk))
                .map(chunkTransformer).toList();

        return joinChunks(chunks);
    }

    // TODO: Create a version that accepts a transformer function
    @Override
    public void write(Path path, byte[] content) throws IOException {
        // If the file is smaller than the chunk size, write it all at once
        if (content.length <= CHUNK_SIZE) {
            writeTotalChunks(path, 1);
            doWrite(path, 0, content, 0, content.length);
            return;
        }

        int totalChunks = (int) Math.ceil((double) content.length / CHUNK_SIZE);

        writeTotalChunks(path, totalChunks);

        @SuppressWarnings("unchecked")
        CompletableFuture<Void>[] futures = new CompletableFuture[totalChunks];

        for (int currentChunk = 0; currentChunk < totalChunks; currentChunk++) {
            int start = currentChunk * CHUNK_SIZE;
            int end = Math.min(content.length - start, CHUNK_SIZE);

            int finalCurrentChunk = currentChunk;
            futures[currentChunk] = CompletableFuture.runAsync(
                    () -> doWrite(path, finalCurrentChunk, content, start, end));
        }

        // Wait until all chunks are written
        CompletableFuture.allOf(futures);
    }

    private int checkTotalChunks(Path path) throws IOException {
        String content = Files.readString(getTotalChunksPath(path));
        return Integer.parseInt(content);
    }

    @SneakyThrows({IOException.class})
    private byte[] doRead(Path path, int chunkNumber) {
        Path chunkPath = getChunkPath(path, chunkNumber);
        return Files.readAllBytes(chunkPath);
    }

    private byte[] joinChunks(List<byte[]> chunks) {
        int fileSize = chunks.stream().mapToInt(chunk -> chunk.length).sum();
        byte[] file = new byte[fileSize];
        int i = 0;
        for (byte[] chunk : chunks) {
            for (byte byyte : chunk) {
                file[i] = byyte;
                i++;
            }
        }
        return file;
    }

    private void writeTotalChunks(Path path, int totalChunks) throws IOException {
        Path totalChunksPath = getTotalChunksPath(path);

        Files.createDirectory(totalChunksPath.getParent());
        Files.createFile(totalChunksPath);
        Files.writeString(totalChunksPath, String.valueOf(totalChunks));
    }

    @SneakyThrows({IOException.class})
    private void doWrite(Path path, int chunkNumber, byte[] wholeContent, int start, int end) {
        Path chunkPath = getChunkPath(path, chunkNumber);
        Files.createFile(chunkPath);

        @Cleanup FileOutputStream stream = new FileOutputStream(chunkPath.toFile());
        @Cleanup FileChannel channel = stream.getChannel();

        ByteBuffer buffer = ByteBuffer.wrap(wholeContent, start, end);
        channel.write(buffer);
    }

    private Path getChunkPath(Path path, int chunkNumber) {
        return Path.of(String.format(CHUNK_NAME_TEMPLATE, path, chunkNumber));
    }

    private Path getTotalChunksPath(Path path) {
        return Path.of(String.format(TOTAL_CHUNKS_NAME_TEMPLATE, path));
    }
}
