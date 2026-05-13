package io.github.stellhub.stellflow.storage.log;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.zip.CRC32;

/**
 * 单段日志文件实现。
 */
public class LogSegment implements AutoCloseable {

    private static final int ENTRY_MAGIC = 0x5354464C;
    private static final int ENTRY_HEADER_BYTES =
            Integer.BYTES
                    + Long.BYTES
                    + Long.BYTES
                    + Integer.BYTES
                    + Integer.BYTES
                    + Integer.BYTES;

    private final long baseOffset;
    private final Path logFile;
    private final FileChannel fileChannel;
    private final OffsetIndex offsetIndex;
    private final TimeIndex timeIndex;
    private final int maxSegmentBytes;
    private final int indexIntervalBytes;
    private final boolean flushEveryAppend;
    private final NavigableMap<Long, EntryMetadata> entries = new TreeMap<>();

    private long validSize;
    private long nextOffset;

    public LogSegment(Path logFile, long baseOffset, int maxSegmentBytes, int indexIntervalBytes) {
        this(logFile, baseOffset, maxSegmentBytes, indexIntervalBytes, true);
    }

    public LogSegment(
            Path logFile,
            long baseOffset,
            int maxSegmentBytes,
            int indexIntervalBytes,
            boolean flushEveryAppend) {
        try {
            Files.createDirectories(logFile.getParent());
            this.baseOffset = baseOffset;
            this.logFile = logFile;
            this.maxSegmentBytes = maxSegmentBytes;
            this.indexIntervalBytes = indexIntervalBytes;
            this.flushEveryAppend = flushEveryAppend;
            this.fileChannel =
                    FileChannel.open(
                            logFile,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.READ,
                            StandardOpenOption.WRITE);
            this.offsetIndex = new OffsetIndex(indexFile(logFile));
            this.timeIndex = new TimeIndex(timeIndexFile(logFile));
            recover();
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to open log segment " + logFile, exception);
        }
    }

    /**
     * 以 leader 身份追加一批记录。
     */
    public synchronized LogAppendResult append(byte[] records, long appendTimestamp, int leaderEpoch) {
        byte[] safeRecords = records == null ? new byte[0] : records.clone();
        if (!canAppend(safeRecords.length) && !entries.isEmpty()) {
            throw new IllegalStateException("Segment " + logFile + " is full and must roll first");
        }
        long offset = nextOffset;
        writeEntry(offset, appendTimestamp, leaderEpoch, safeRecords);
        EntryMetadata entryMetadata =
                new EntryMetadata(validSize, safeRecords.length, appendTimestamp, leaderEpoch);
        entries.put(offset, entryMetadata);
        rebuildIndexes();
        validSize += ENTRY_HEADER_BYTES + safeRecords.length;
        nextOffset = offset + 1;
        return new LogAppendResult(offset, nextOffset, nextOffset, leaderEpoch);
    }

    /**
     * 以 follower 身份追加单条复制记录。
     */
    public synchronized void appendReplicaEntry(ReplicaLogEntry entry) {
        if (entry.offset() != nextOffset) {
            throw new IllegalArgumentException(
                    "Replica offset mismatch. expected=" + nextOffset + ", actual=" + entry.offset());
        }
        byte[] safeRecords = entry.records() == null ? new byte[0] : entry.records().clone();
        if (!canAppend(safeRecords.length) && !entries.isEmpty()) {
            throw new IllegalStateException("Segment " + logFile + " is full and must roll first");
        }
        writeEntry(entry.offset(), entry.timestamp(), entry.leaderEpoch(), safeRecords);
        entries.put(
                entry.offset(),
                new EntryMetadata(validSize, safeRecords.length, entry.timestamp(), entry.leaderEpoch()));
        rebuildIndexes();
        validSize += ENTRY_HEADER_BYTES + safeRecords.length;
        nextOffset = entry.offset() + 1;
    }

    /**
     * 从指定 offset 开始读取复制记录。
     */
    public synchronized SegmentReplicaReadResult readReplica(long fetchOffset, int maxBytes) {
        if (fetchOffset >= nextOffset || maxBytes <= 0) {
            return new SegmentReplicaReadResult(fetchOffset, java.util.List.of());
        }
        long startPosition = offsetIndex.lookupPosition(fetchOffset, 0);
        java.util.List<ReplicaLogEntry> entries = new java.util.ArrayList<>();
        long nextReadableOffset = fetchOffset;
        long position = startPosition;
        int totalBytes = 0;
        while (position + ENTRY_HEADER_BYTES <= validSize) {
            EntryReadResult entryReadResult = readEntry(position);
            long offset = entryReadResult.offset();
            if (offset < fetchOffset) {
                position = entryReadResult.nextPosition();
                continue;
            }
            int entryBytes = ENTRY_HEADER_BYTES + entryReadResult.records().length;
            if (totalBytes + entryBytes > maxBytes && !entries.isEmpty()) {
                break;
            }
            entries.add(
                    new ReplicaLogEntry(
                            entryReadResult.offset(),
                            entryReadResult.timestamp(),
                            entryReadResult.leaderEpoch(),
                            entryReadResult.records()));
            totalBytes += entryBytes;
            nextReadableOffset = offset + 1;
            position = entryReadResult.nextPosition();
        }
        return new SegmentReplicaReadResult(nextReadableOffset, entries);
    }

    /**
     * 列出从指定 offset 开始的最多 N 个 offset。
     */
    public synchronized java.util.List<Long> listOffsetsFrom(long startOffset, int maxNumOffsets) {
        java.util.List<Long> offsets = new java.util.ArrayList<>(Math.max(maxNumOffsets, 0));
        for (Long offset : entries.tailMap(startOffset, true).navigableKeySet()) {
            if (offsets.size() >= maxNumOffsets) {
                break;
            }
            offsets.add(offset);
        }
        return offsets;
    }

    /**
     * 追加一条标准 entry。
     */
    private void writeEntry(long offset, long appendTimestamp, int leaderEpoch, byte[] safeRecords) {
        int crc = crc32(safeRecords);
        ByteBuffer headerBuffer = ByteBuffer.allocate(ENTRY_HEADER_BYTES);
        headerBuffer.putInt(ENTRY_MAGIC);
        headerBuffer.putLong(offset);
        headerBuffer.putLong(appendTimestamp);
        headerBuffer.putInt(leaderEpoch);
        headerBuffer.putInt(safeRecords.length);
        headerBuffer.putInt(crc);
        headerBuffer.flip();
        try {
            fileChannel.position(validSize);
            writeFully(headerBuffer);
            writeFully(ByteBuffer.wrap(safeRecords));
            if (flushEveryAppend) {
                fileChannel.force(false);
            }
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to append records into " + logFile, exception);
        }
    }

    /**
     * 从指定 offset 开始读取多个批次。
     */
    public synchronized SegmentReadResult read(long fetchOffset, int maxBytes) {
        if (fetchOffset >= nextOffset || maxBytes <= 0) {
            return new SegmentReadResult(fetchOffset, new byte[0]);
        }
        long startPosition = offsetIndex.lookupPosition(fetchOffset, 0);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        long nextReadableOffset = fetchOffset;
        long position = startPosition;
        while (position + ENTRY_HEADER_BYTES <= validSize) {
            EntryReadResult entryReadResult = readEntry(position);
            long offset = entryReadResult.offset();
            if (offset < fetchOffset) {
                position = entryReadResult.nextPosition();
                continue;
            }
            if (outputStream.size() + entryReadResult.records().length > maxBytes
                    && outputStream.size() > 0) {
                break;
            }
            try {
                outputStream.write(entryReadResult.records());
            } catch (IOException exception) {
                throw new UncheckedIOException(exception);
            }
            nextReadableOffset = offset + 1;
            position = entryReadResult.nextPosition();
        }
        return new SegmentReadResult(nextReadableOffset, outputStream.toByteArray());
    }

    /**
     * 当前段的起始 offset。
     */
    public long baseOffset() {
        return baseOffset;
    }

    /**
     * 当前段的下一个 offset。
     */
    public synchronized long nextOffset() {
        return nextOffset;
    }

    /**
     * 当前段是否包含指定 offset。
     */
    public synchronized boolean mayContain(long offset) {
        return offset >= baseOffset && offset < nextOffset;
    }

    /**
     * 当前段是否为空。
     */
    public synchronized boolean isEmpty() {
        return entries.isEmpty();
    }

    /**
     * 当前段文件有效大小。
     */
    public synchronized long sizeInBytes() {
        return validSize;
    }

    /**
     * 当前段最大时间戳对应的 offset。
     */
    public synchronized long lookupOffsetByTimestamp(long timestamp) {
        return timeIndex.lookupOffset(timestamp, baseOffset);
    }

    /**
     * 返回第一个时间戳不小于目标值的 offset。
     */
    public synchronized long findOffsetByTimestamp(long timestamp) {
        for (var entry : entries.entrySet()) {
            if (entry.getValue().appendTimestamp() >= timestamp) {
                return entry.getKey();
            }
        }
        return nextOffset;
    }

    /**
     * 返回第一个时间戳不小于目标值的 offset 与时间戳。
     */
    public synchronized TimestampOffsetResult findTimestampOffset(long timestamp) {
        for (var entry : entries.entrySet()) {
            if (entry.getValue().appendTimestamp() >= timestamp) {
                return new TimestampOffsetResult(entry.getKey(), entry.getValue().appendTimestamp());
            }
        }
        return new TimestampOffsetResult(nextOffset, maxTimestamp());
    }

    /**
     * 返回当前段中每个 epoch 的首条 offset。
     */
    public synchronized NavigableMap<Integer, Long> epochStartOffsets() {
        NavigableMap<Integer, Long> result = new TreeMap<>();
        for (var entry : entries.entrySet()) {
            result.putIfAbsent(entry.getValue().leaderEpoch(), entry.getKey());
        }
        return result;
    }

    /**
     * 返回当前段最大时间戳。
     */
    public synchronized long maxTimestamp() {
        if (entries.isEmpty()) {
            return 0;
        }
        return entries.lastEntry().getValue().appendTimestamp();
    }

    /**
     * 判断当前段是否还能容纳指定记录。
     */
    public synchronized boolean canAppend(int recordLength) {
        long entryBytes = ENTRY_HEADER_BYTES + (long) recordLength;
        return validSize + entryBytes <= maxSegmentBytes || entries.isEmpty();
    }

    /**
     * 将当前段截断到指定 offset。
     */
    public synchronized void truncateTo(long offset) {
        if (offset <= baseOffset) {
            entries.clear();
            validSize = 0;
            nextOffset = baseOffset;
        } else {
            EntryMetadata truncatePoint = entries.get(offset);
            long truncatePosition = truncatePoint != null ? truncatePoint.position() : validSize;
            entries.tailMap(offset, true).clear();
            validSize = truncatePosition;
            nextOffset = offset;
        }
        try {
            fileChannel.truncate(validSize);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to truncate log segment " + logFile, exception);
        }
        rebuildIndexes();
    }

    /**
     * 删除当前段及其索引文件。
     */
    public synchronized void deleteFiles() {
        close();
        try {
            Files.deleteIfExists(logFile);
            Files.deleteIfExists(indexFile(logFile));
            Files.deleteIfExists(timeIndexFile(logFile));
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to delete log segment files for " + logFile, exception);
        }
    }

    /**
     * 启动恢复、CRC 校验与尾部截断。
     */
    private void recover() throws IOException {
        entries.clear();
        validSize = 0;
        nextOffset = baseOffset;
        long previousOffset = baseOffset - 1;
        long fileSize = fileChannel.size();
        long position = 0;
        while (position + ENTRY_HEADER_BYTES <= fileSize) {
            ByteBuffer headerBuffer = ByteBuffer.allocate(ENTRY_HEADER_BYTES);
            fileChannel.position(position);
            if (readFully(headerBuffer) < ENTRY_HEADER_BYTES) {
                break;
            }
            headerBuffer.flip();
            int magic = headerBuffer.getInt();
            long offset = headerBuffer.getLong();
            long timestamp = headerBuffer.getLong();
            int leaderEpoch = headerBuffer.getInt();
            int recordLength = headerBuffer.getInt();
            int crc = headerBuffer.getInt();
            if (magic != ENTRY_MAGIC
                    || recordLength < 0
                    || offset <= previousOffset
                    || offset < baseOffset
                    || position + ENTRY_HEADER_BYTES + recordLength > fileSize) {
                break;
            }
            ByteBuffer recordBuffer = ByteBuffer.allocate(recordLength);
            if (readFully(recordBuffer) < recordLength) {
                break;
            }
            byte[] records = recordBuffer.array();
            if (crc32(records) != crc) {
                break;
            }
            entries.put(offset, new EntryMetadata(position, recordLength, timestamp, leaderEpoch));
            previousOffset = offset;
            validSize = position + ENTRY_HEADER_BYTES + recordLength;
            nextOffset = offset + 1;
            position = validSize;
        }
        if (validSize != fileSize) {
            fileChannel.truncate(validSize);
        }
        rebuildIndexes();
    }

    /**
     * 按当前位置读取单条 entry。
     */
    private EntryReadResult readEntry(long position) {
        try {
            ByteBuffer headerBuffer = ByteBuffer.allocate(ENTRY_HEADER_BYTES);
            fileChannel.position(position);
            readFully(headerBuffer);
            headerBuffer.flip();
            int magic = headerBuffer.getInt();
            long offset = headerBuffer.getLong();
            long timestamp = headerBuffer.getLong();
            int leaderEpoch = headerBuffer.getInt();
            int recordLength = headerBuffer.getInt();
            int crc = headerBuffer.getInt();
            if (magic != ENTRY_MAGIC || recordLength < 0) {
                throw new IllegalStateException("Invalid entry header in " + logFile);
            }
            ByteBuffer recordBuffer = ByteBuffer.allocate(recordLength);
            readFully(recordBuffer);
            byte[] records = recordBuffer.array();
            if (crc32(records) != crc) {
                throw new IllegalStateException("CRC validation failed for " + logFile);
            }
            return new EntryReadResult(
                    offset,
                    timestamp,
                    leaderEpoch,
                    records,
                    position + ENTRY_HEADER_BYTES + recordLength);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to read entry from " + logFile, exception);
        }
    }

    /**
     * 重建 offset/time 索引。
     */
    private void rebuildIndexes() {
        NavigableMap<Long, Long> offsetEntries = new TreeMap<>();
        NavigableMap<Long, Long> timeEntries = new TreeMap<>();
        long lastIndexedPosition = Long.MIN_VALUE;
        for (var entry : entries.entrySet()) {
            EntryMetadata metadata = entry.getValue();
            if (offsetEntries.isEmpty()
                    || metadata.position() - lastIndexedPosition >= indexIntervalBytes) {
                offsetEntries.put(entry.getKey(), metadata.position());
                lastIndexedPosition = metadata.position();
            }
            timeEntries.put(metadata.appendTimestamp(), entry.getKey());
        }
        offsetIndex.rebuild(offsetEntries);
        timeIndex.rebuild(timeEntries);
    }

    /**
     * 计算记录 CRC32。
     */
    private int crc32(byte[] records) {
        CRC32 crc32 = new CRC32();
        crc32.update(records);
        return (int) crc32.getValue();
    }

    /**
     * 派生 offset 索引文件路径。
     */
    private static Path indexFile(Path logFile) {
        return siblingWithExtension(logFile, ".index");
    }

    /**
     * 派生时间索引文件路径。
     */
    private static Path timeIndexFile(Path logFile) {
        return siblingWithExtension(logFile, ".timeindex");
    }

    /**
     * 根据扩展名生成同名兄弟文件。
     */
    private static Path siblingWithExtension(Path logFile, String extension) {
        String fileName = logFile.getFileName().toString();
        int dot = fileName.lastIndexOf('.');
        String baseName = dot >= 0 ? fileName.substring(0, dot) : fileName;
        return logFile.resolveSibling(baseName + extension);
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
            offsetIndex.close();
            timeIndex.close();
            fileChannel.close();
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to close log segment " + logFile, exception);
        }
    }

    /**
     * 段内 entry 元数据。
     */
    private record EntryMetadata(
            long position, int recordLength, long appendTimestamp, int leaderEpoch) {}

    /**
     * 单条 entry 读取结果。
     */
    private record EntryReadResult(
            long offset,
            long timestamp,
            int leaderEpoch,
            byte[] records,
            long nextPosition) {}

    /**
     * 段级批量读取结果。
     */
    public record SegmentReadResult(long nextOffset, byte[] records) {}

    /**
     * 段级复制读取结果。
     */
    public record SegmentReplicaReadResult(long nextOffset, java.util.List<ReplicaLogEntry> entries) {}
}
