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
 * @apiNote Is it possible to "generify" {@code read(Path, Function<byte[], T>)}? That
 * way the transform functions can return values other than {@code byte[]}.
 */
@Service
public class ChunkedFileService implements FileService {
    /**
     * Size in bytes: 10 MiB.
     */
    private static final int CHUNK_SIZE = 1024 * 1024 * 10;

    private static final String CHUNK_NAME_TEMPLATE = "%s/chunk%s";
    private static final String TOTAL_CHUNKS_NAME_TEMPLATE = "%s/total_chunks";

    /**
     * Reads all chunks of a file, and then merges them into a single byte array.
     *
     * @param path Path to the file
     * @return The file contents
     * @throws IOException When there is an error in getting the number of
     *                     chunks to read, or there's an error when reading the chunks
     */
    @Override
    public byte[] read(Path path) throws IOException {
        return read(path, Function.identity());
    }

    /**
     * Reads all chunks of a file, giving you the option to transform them
     * (e.g. to decrypt them). After all chunks are read and transformed,
     * they are merged into a single byte array.
     *
     * @param path             Path to the file
     * @param chunkTransformer Function that transforms each chunk
     * @return The file contents
     * @throws IOException When there is an error in getting the number of
     *                     chunks to read, or there's an error when reading the chunks
     */
    public byte[] read(Path path, Function<byte[], byte[]> chunkTransformer) throws IOException {
        // Get the total of chunks that have to be read
        int totalChunks = checkTotalChunks(path);

        // Read each chunk in parallel and transform its content
        List<byte[]> chunks = IntStream.range(0, totalChunks).parallel()
                .mapToObj(currentChunk -> doRead(path, currentChunk))
                .map(chunkTransformer).toList();

        return joinChunks(chunks);
    }

    /**
     * Writes content to a file. The content is written in chunks, so it can
     * later be read/transformed in parallel.
     * <p>
     * TODO: Create a version that accepts a transformer function like read() does
     *
     * @param path    The path to the file
     * @param content The content to be written in chunks
     * @throws IOException When there's an error in writing the chunks or the _total_ chunks
     */
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
            // Get the start and end of the current chunk
            int start = currentChunk * CHUNK_SIZE;
            int end = Math.min(content.length - start, CHUNK_SIZE);

            int finalCurrentChunk = currentChunk;

            // Write each chunk in parallel
            futures[currentChunk] = CompletableFuture.runAsync(
                    () -> doWrite(path, finalCurrentChunk, content, start, end));
        }

        // Wait until all chunks are written
        CompletableFuture.allOf(futures);
    }

    /**
     * Checks how many chunks do we need to read from a file.
     */
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
        // Join all chunks into a single byte array
        for (byte[] chunk : chunks) {
            for (byte byyte : chunk) {
                file[i] = byyte;
                i++;
            }
        }
        return file;
    }

    /**
     * Writes the file containing how many chunks we need to read to get the
     * contents of a file.
     */
    private void writeTotalChunks(Path path, int totalChunks) throws IOException {
        Path totalChunksPath = getTotalChunksPath(path);

        Files.createDirectory(totalChunksPath.getParent());
        Files.createFile(totalChunksPath);
        Files.writeString(totalChunksPath, String.valueOf(totalChunks));
    }

    /**
     * Writes a chunk using a FileChannel and a ByteBuffer (it's fast).
     */
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
