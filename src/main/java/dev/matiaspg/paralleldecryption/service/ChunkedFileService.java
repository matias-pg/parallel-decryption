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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.IntStream;

@Service
public class ChunkedFileService implements FileService {
    /**
     * Size in bytes: 10 MiB.
     */
    private static final int CHUNK_SIZE = 1024 * 1024 * 10;

    private static final String CHUNK_NAME_TEMPLATE = "%s.chunked/chunk%s";
    private static final String TOTAL_CHUNKS_NAME_TEMPLATE = "%s.chunked/total_chunks";

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
     * @implNote Is it possible to "generify" it as {@code read<T>(Path, Function<byte[], T>)}? That
     * way the transform function can return values other than {@code byte[]}.
     */
    public byte[] read(Path path, Function<byte[], byte[]> chunkTransformer) throws IOException {
        // Get the total of chunks that have to be read
        int totalChunks = checkTotalChunks(path);

        // Read the chunks and transform their content in parallel
        List<byte[]> chunks = IntStream.range(0, totalChunks).parallel()
                .mapToObj(currentChunk -> doRead(path, currentChunk))
                .map(chunkTransformer).toList();

        return joinChunks(chunks);
    }

    /**
     * Writes content to a file. The content is written in chunks, so it can
     * later be read and optionally transformed in parallel.
     *
     * @param path    The path to the file
     * @param content The content to be written in chunks
     * @throws IOException When there's an error in writing the chunks or the _total_ chunks
     */
    @Override
    public void write(Path path, byte[] content) throws IOException {
        write(path, content, null);
    }

    /**
     * Writes content to a file. The content is written in chunks, so it can
     * later be read and optionally transformed in parallel.
     * <p>
     * This method also allows chunks to be transformed in parallel (e.g. to
     * encrypt them) before they are written.
     *
     * @param path    The path to the file
     * @param content The content to be written in chunks
     * @throws IOException When there's an error in writing the chunks or the _total_ chunks
     */
    public void write(Path path, byte[] content, Function<byte[], byte[]> chunkTransformer) throws IOException {
        // If the file is smaller than the chunk size, write it all at once
        if (content.length <= CHUNK_SIZE) {
            writeTotalChunks(path, 1);
            doWrite(path, 0, content, 0, content.length, chunkTransformer);
            return;
        }

        int totalChunks = (int) Math.ceil((double) content.length / CHUNK_SIZE);

        writeTotalChunks(path, totalChunks);

        // Write the chunks and optionally transform their content in parallel
        @SuppressWarnings("unchecked")
        CompletableFuture<Void>[] futures = IntStream.range(0, totalChunks).mapToObj(currentChunk -> {
            // Get the offset and length of the current chunk
            int offset = currentChunk * CHUNK_SIZE;
            int length = Math.min(content.length - offset, CHUNK_SIZE);

            // Write the chunk asynchronously
            return CompletableFuture.runAsync(() -> doWrite(
                    path, currentChunk,
                    content, offset, length,
                    chunkTransformer
            ));
        }).toArray(CompletableFuture[]::new);

        // Wait until all chunks are written
        CompletableFuture.allOf(futures);
    }

    /**
     * Checks how many chunks have to be read to get the contents of a file.
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
        // The file size is the sum of the sizes of all chunks
        int fileSize = chunks.stream().mapToInt(chunk -> chunk.length).sum();
        byte[] file = new byte[fileSize];
        int i = 0;
        // Join all chunks into a single byte array
        for (byte[] chunk : chunks) {
            for (byte chunkByte : chunk) {
                file[i] = chunkByte;
                ++i;
            }
        }
        return file;
    }

    /**
     * Writes the file containing how many chunks have to be read to get the
     * contents of a file.
     */
    private void writeTotalChunks(Path path, int totalChunks) throws IOException {
        Path totalChunksPath = getTotalChunksPath(path);

        Files.createDirectory(totalChunksPath.getParent());
        Files.createFile(totalChunksPath);
        Files.writeString(totalChunksPath, String.valueOf(totalChunks));
    }

    /**
     * Writes a chunk of a file, optionally transforming it before it's written.
     */
    @SneakyThrows({IOException.class})
    private void doWrite(
            Path path, int chunkNumber,
            byte[] wholeContent, int offset, int length,
            Function<byte[], byte[]> chunkTransformer
    ) {
        Path chunkPath = getChunkPath(path, chunkNumber);
        Files.createFile(chunkPath);

        @Cleanup FileOutputStream stream = new FileOutputStream(chunkPath.toFile());
        @Cleanup FileChannel channel = stream.getChannel();

        if (chunkTransformer != null) {
            byte[] chunk = Arrays.copyOfRange(wholeContent, offset, offset + length);
            byte[] transformedChunk = chunkTransformer.apply(chunk);
            channel.write(ByteBuffer.wrap(transformedChunk));
        } else {
            ByteBuffer buffer = ByteBuffer.wrap(wholeContent, offset, length);
            channel.write(buffer);
        }
    }

    private Path getChunkPath(Path path, int chunkNumber) {
        return Path.of(String.format(CHUNK_NAME_TEMPLATE, path, chunkNumber));
    }

    private Path getTotalChunksPath(Path path) {
        return Path.of(String.format(TOTAL_CHUNKS_NAME_TEMPLATE, path));
    }
}
