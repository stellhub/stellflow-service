package io.github.stellhub.stellflow.storage.log;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * 稀疏 offset 索引。
 */
public class OffsetIndex implements AutoCloseable {

    private static final int ENTRY_BYTES = Long.BYTES + Long.BYTES;

    private final Path indexFile;
    private final FileChannel fileChannel;
    private final NavigableMap<Long, Long> entries = new TreeMap<>();

    public OffsetIndex(Path indexFile) {
        try {
            Files.createDirectories(indexFile.getParent());
            this.indexFile = indexFile;
            this.fileChannel =
                    FileChannel.open(
                            indexFile,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.READ,
                            StandardOpenOption.WRITE);
            recover();
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to open offset index " + indexFile, exception);
        }
    }

    /**
     * 追加索引项。
     */
    public synchronized void append(long offset, long position) {
        Long existing = entries.get(offset);
        if (existing != null && existing == position) {
            return;
        }
        ByteBuffer buffer = ByteBuffer.allocate(ENTRY_BYTES);
        buffer.putLong(offset);
        buffer.putLong(position);
        buffer.flip();
        try {
            fileChannel.position(fileChannel.size());
            writeFully(buffer);
            fileChannel.force(false);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to append offset index " + indexFile, exception);
        }
        entries.put(offset, position);
    }

    /**
     * 重建索引内容。
     */
    public synchronized void rebuild(NavigableMap<Long, Long> sourceEntries) {
        entries.clear();
        try {
            fileChannel.truncate(0);
            fileChannel.position(0);
            for (Map.Entry<Long, Long> entry : sourceEntries.entrySet()) {
                ByteBuffer buffer = ByteBuffer.allocate(ENTRY_BYTES);
                buffer.putLong(entry.getKey());
                buffer.putLong(entry.getValue());
                buffer.flip();
                writeFully(buffer);
                entries.put(entry.getKey(), entry.getValue());
            }
            fileChannel.force(false);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to rebuild offset index " + indexFile, exception);
        }
    }

    /**
     * 根据 offset 查找最接近的位置。
     */
    public synchronized long lookupPosition(long offset, long defaultPosition) {
        Map.Entry<Long, Long> floorEntry = entries.floorEntry(offset);
        if (floorEntry == null) {
            return defaultPosition;
        }
        return floorEntry.getValue();
    }

    /**
     * 返回索引项数量。
     */
    public synchronized int size() {
        return entries.size();
    }

    /**
     * 恢复索引文件。
     */
    private void recover() throws IOException {
        entries.clear();
        long fileSize = fileChannel.size();
        long position = 0;
        while (position + ENTRY_BYTES <= fileSize) {
            ByteBuffer buffer = ByteBuffer.allocate(ENTRY_BYTES);
            fileChannel.position(position);
            if (readFully(buffer) < ENTRY_BYTES) {
                break;
            }
            buffer.flip();
            entries.put(buffer.getLong(), buffer.getLong());
            position += ENTRY_BYTES;
        }
        if (position != fileSize) {
            fileChannel.truncate(position);
        }
    }

    /**
     * 写满缓冲区。
     */
    private void writeFully(ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            fileChannel.write(buffer);
        }
    }

    /**
     * 尽量读满缓冲区。
     */
    private int readFully(ByteBuffer buffer) throws IOException {
        int totalRead = 0;
        while (buffer.hasRemaining()) {
            int read = fileChannel.read(buffer);
            if (read < 0) {
                break;
            }
            totalRead += read;
        }
        return totalRead;
    }

    @Override
    public synchronized void close() {
        try {
            fileChannel.close();
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to close offset index " + indexFile, exception);
        }
    }
}
