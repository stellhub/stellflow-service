package io.github.stellhub.stellflow.storage.log;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 单个 TopicPartition 的统一日志入口。
 */
public class UnifiedLog implements AutoCloseable {

    private static final String LOG_EXTENSION = ".log";
    private static final String CHECKPOINT_FILE_NAME = "log.checkpoint";
    private static final String LEADER_EPOCH_CHECKPOINT_FILE_NAME = "leader-epoch.checkpoint";
    private static final int LOCAL_BROKER_ID = 0;

    private final TopicPartition topicPartition;
    private final Path partitionDir;
    private final int segmentBytes;
    private final int indexIntervalBytes;
    private final int retentionSegments;
    private final long retentionMs;
    private final long retentionBytes;
    private final LogCheckpoint checkpoint;
    private final LeaderEpochCheckpoint leaderEpochCheckpoint;
    private final List<LogSegment> segments = new ArrayList<>();
    private final Map<Integer, Long> replicaEndOffsets = new ConcurrentHashMap<>();
    private final Set<Integer> replicaNodes = new TreeSet<>();
    private final Set<Integer> isr = new TreeSet<>();

    private long highWatermark;
    private long logEndOffset;
    private long logStartOffset;
    private int leaderEpoch;
    private int leaderId = LOCAL_BROKER_ID;

    public UnifiedLog(
            TopicPartition topicPartition,
            Path partitionDir,
            int segmentBytes,
            int indexIntervalBytes,
            int retentionSegments,
            long retentionMs,
            long retentionBytes) {
        this.topicPartition = topicPartition;
        this.partitionDir = partitionDir;
        this.segmentBytes = segmentBytes;
        this.indexIntervalBytes = indexIntervalBytes;
        this.retentionSegments = retentionSegments;
        this.retentionMs = retentionMs;
        this.retentionBytes = retentionBytes;
        this.checkpoint = new LogCheckpoint(partitionDir.resolve(CHECKPOINT_FILE_NAME));
        this.leaderEpochCheckpoint =
                new LeaderEpochCheckpoint(partitionDir.resolve(LEADER_EPOCH_CHECKPOINT_FILE_NAME));
        initialize();
    }

    /**
     * 以 Leader 方式追加。
     */
    public synchronized LogAppendResult appendAsLeader(byte[] records) {
        byte[] safeRecords = records == null ? new byte[0] : records.clone();
        LogSegment activeSegment = activeSegment();
        if (!activeSegment.canAppend(safeRecords.length) && !activeSegment.isEmpty()) {
            activeSegment = roll(logEndOffset);
        }
        leaderEpochCheckpoint.assignIfAbsent(leaderEpoch, logEndOffset);
        LogAppendResult appendResult =
                activeSegment.append(safeRecords, System.currentTimeMillis(), leaderEpoch);
        logEndOffset = appendResult.logEndOffset();
        replicaEndOffsets.put(leaderId, logEndOffset);
        maybeApplyRetention(System.currentTimeMillis());
        recomputeHighWatermark();
        persistCheckpoint();
        return new LogAppendResult(appendResult.baseOffset(), logEndOffset, highWatermark, leaderEpoch);
    }

    /**
     * 读取可见数据。
     */
    public synchronized LogReadResult read(long fetchOffset, int maxBytes) {
        long effectiveFetchOffset = Math.max(fetchOffset, logStartOffset);
        if (maxBytes <= 0 || effectiveFetchOffset >= logEndOffset) {
            return new LogReadResult(
                    highWatermark,
                    logStartOffset,
                    highWatermark,
                    effectiveFetchOffset,
                    new byte[0]);
        }
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        long nextFetchOffset = effectiveFetchOffset;
        for (LogSegment segment : segments) {
            if (nextFetchOffset >= segment.nextOffset()) {
                continue;
            }
            if (nextFetchOffset < segment.baseOffset() && outputStream.size() == 0) {
                nextFetchOffset = segment.baseOffset();
            }
            LogSegment.SegmentReadResult segmentReadResult =
                    segment.read(nextFetchOffset, maxBytes - outputStream.size());
            if (segmentReadResult.records().length == 0) {
                continue;
            }
            try {
                outputStream.write(segmentReadResult.records());
            } catch (IOException exception) {
                throw new UncheckedIOException(exception);
            }
            nextFetchOffset = segmentReadResult.nextOffset();
            if (outputStream.size() >= maxBytes) {
                break;
            }
        }
        return new LogReadResult(
                highWatermark,
                logStartOffset,
                highWatermark,
                nextFetchOffset,
                outputStream.toByteArray());
    }

    /**
     * 读取副本同步数据。
     */
    public synchronized ReplicaLogReadResult readReplica(long fetchOffset, int maxBytes) {
        long effectiveFetchOffset = Math.max(fetchOffset, logStartOffset);
        if (maxBytes <= 0 || effectiveFetchOffset >= logEndOffset) {
            return new ReplicaLogReadResult(
                    highWatermark, logStartOffset, highWatermark, effectiveFetchOffset, List.of());
        }
        List<ReplicaLogEntry> entries = new ArrayList<>();
        long nextFetchOffset = effectiveFetchOffset;
        int totalBytes = 0;
        for (LogSegment segment : segments) {
            if (nextFetchOffset >= segment.nextOffset()) {
                continue;
            }
            if (nextFetchOffset < segment.baseOffset() && entries.isEmpty()) {
                nextFetchOffset = segment.baseOffset();
            }
            LogSegment.SegmentReplicaReadResult segmentReadResult =
                    segment.readReplica(nextFetchOffset, maxBytes - totalBytes);
            if (segmentReadResult.entries().isEmpty()) {
                continue;
            }
            for (ReplicaLogEntry entry : segmentReadResult.entries()) {
                totalBytes += 24 + entry.records().length;
                entries.add(entry);
            }
            nextFetchOffset = segmentReadResult.nextOffset();
            if (totalBytes >= maxBytes) {
                break;
            }
        }
        return new ReplicaLogReadResult(
                highWatermark, logStartOffset, highWatermark, nextFetchOffset, entries);
    }

    /**
     * 追加 follower 从 leader 获取的复制数据。
     */
    public synchronized void appendReplicaEntries(List<ReplicaLogEntry> replicaEntries, int leaderEpoch) {
        if (replicaEntries == null || replicaEntries.isEmpty()) {
            updateLeaderEpoch(leaderEpoch);
            return;
        }
        updateLeaderEpoch(leaderEpoch);
        for (ReplicaLogEntry entry : replicaEntries) {
            LogSegment activeSegment = activeSegment();
            if (!activeSegment.canAppend(entry.records().length) && !activeSegment.isEmpty()) {
                activeSegment = roll(logEndOffset);
            }
            leaderEpochCheckpoint.assignIfAbsent(entry.leaderEpoch(), entry.offset());
            activeSegment.appendReplicaEntry(entry);
            logEndOffset = entry.offset() + 1;
        }
        replicaEndOffsets.put(leaderId, logEndOffset);
        maybeApplyRetention(System.currentTimeMillis());
        recomputeHighWatermark();
        persistCheckpoint();
    }

    /**
     * 根据时间戳查找 offset。
     */
    public synchronized TimestampOffsetResult findOffsetByTimestamp(long timestamp) {
        for (LogSegment segment : segments) {
            if (segment.isEmpty()) {
                continue;
            }
            if (timestamp <= segment.maxTimestamp()) {
                TimestampOffsetResult result = segment.findTimestampOffset(timestamp);
                return new TimestampOffsetResult(
                        Math.max(segment.baseOffset(), result.offset()), result.timestamp());
            }
        }
        return new TimestampOffsetResult(logEndOffset, segments.isEmpty() ? 0 : activeSegment().maxTimestamp());
    }

    /**
     * 根据时间戳列出最多 N 个 offset。
     */
    public synchronized List<Long> listOffsetsForTimestamp(long timestamp, int maxNumOffsets) {
        if (maxNumOffsets <= 0) {
            return List.of();
        }
        long startOffset;
        if (timestamp < 0) {
            startOffset = logStartOffset;
        } else {
            startOffset = findOffsetByTimestamp(timestamp).offset();
        }
        if (startOffset >= logEndOffset) {
            return List.of();
        }
        List<Long> offsets = new ArrayList<>(maxNumOffsets);
        for (LogSegment segment : segments) {
            if (offsets.size() >= maxNumOffsets) {
                break;
            }
            if (startOffset >= segment.nextOffset()) {
                continue;
            }
            offsets.addAll(
                    segment.listOffsetsFrom(
                            Math.max(startOffset, segment.baseOffset()),
                            maxNumOffsets - offsets.size()));
        }
        return offsets;
    }

    /**
     * 推进 leader epoch。
     */
    public synchronized void updateLeaderEpoch(int newLeaderEpoch) {
        if (newLeaderEpoch < leaderEpoch) {
            throw new IllegalArgumentException(
                    "leaderEpoch must be monotonic. current="
                            + leaderEpoch
                            + ", new="
                            + newLeaderEpoch);
        }
        if (newLeaderEpoch == leaderEpoch) {
            return;
        }
        leaderEpochCheckpoint.assignIfAbsent(newLeaderEpoch, logEndOffset);
        leaderEpoch = newLeaderEpoch;
        persistCheckpoint();
    }

    /**
     * 设置当前 leader 与副本拓扑。
     */
    public synchronized void updateReplicaTopology(
            int leaderId, Collection<Integer> replicaNodes, Collection<Integer> isr) {
        this.leaderId = leaderId;
        this.replicaNodes.clear();
        this.replicaNodes.addAll(replicaNodes);
        if (!this.replicaNodes.contains(leaderId)) {
            this.replicaNodes.add(leaderId);
        }
        this.isr.clear();
        this.isr.addAll(isr);
        if (this.isr.isEmpty()) {
            this.isr.add(leaderId);
        }
        if (!this.isr.contains(leaderId)) {
            this.isr.add(leaderId);
        }
        replicaEndOffsets.putIfAbsent(leaderId, logEndOffset);
        recomputeHighWatermark();
        persistCheckpoint();
    }

    /**
     * 更新 follower 已复制到的 offset。
     */
    public synchronized void updateReplicaFetchOffset(int brokerId, long replicaEndOffset) {
        replicaEndOffsets.put(brokerId, Math.min(replicaEndOffset, logEndOffset));
        recomputeHighWatermark();
        persistCheckpoint();
    }

    /**
     * 将日志截断到指定 offset。
     */
    public synchronized void truncateTo(long offset) {
        long targetOffset = Math.max(offset, logStartOffset);
        if (targetOffset >= logEndOffset) {
            return;
        }
        List<LogSegment> toDelete = new ArrayList<>();
        LogSegment truncationSegment = null;
        for (LogSegment segment : segments) {
            if (segment.baseOffset() >= targetOffset) {
                toDelete.add(segment);
                continue;
            }
            if (segment.mayContain(targetOffset)) {
                truncationSegment = segment;
            }
        }
        for (LogSegment segment : toDelete) {
            if (segment == activeSegment() && truncationSegment == null && segments.size() == 1) {
                continue;
            }
            segments.remove(segment);
            segment.deleteFiles();
        }
        if (truncationSegment != null) {
            truncationSegment.truncateTo(targetOffset);
        }
        if (segments.isEmpty()) {
            segments.add(createSegment(targetOffset));
        }
        segments.sort(Comparator.comparingLong(LogSegment::baseOffset));
        logEndOffset = activeSegment().nextOffset();
        highWatermark = Math.min(highWatermark, logEndOffset);
        if (logStartOffset > logEndOffset) {
            logStartOffset = logEndOffset;
        }
        replicaEndOffsets.replaceAll((ignored, currentOffset) -> Math.min(currentOffset, logEndOffset));
        replicaEndOffsets.put(leaderId, logEndOffset);
        rebuildLeaderEpochCheckpoint();
        recomputeHighWatermark();
        persistCheckpoint();
    }

    /**
     * 按 epoch 分叉点截断日志。
     */
    public synchronized void truncateToLeaderEpoch(int epoch) {
        Long nextEpochStartOffset = leaderEpochCheckpoint.nextEpochStartOffset(epoch);
        if (nextEpochStartOffset == null || nextEpochStartOffset >= logEndOffset) {
            return;
        }
        truncateTo(nextEpochStartOffset);
    }

    /**
     * 删除早于指定 offset 的完整 segment。
     */
    public synchronized void deleteSegmentsBeforeOffset(long offset) {
        if (segments.size() <= 1) {
            return;
        }
        List<LogSegment> deletable = new ArrayList<>();
        for (int index = 0; index < segments.size() - 1; index++) {
            LogSegment segment = segments.get(index);
            if (segment.nextOffset() <= offset) {
                deletable.add(segment);
            }
        }
        if (deletable.isEmpty()) {
            return;
        }
        for (LogSegment segment : deletable) {
            segments.remove(segment);
            segment.deleteFiles();
        }
        logStartOffset = segments.get(0).baseOffset();
        replicaEndOffsets.replaceAll((ignored, currentOffset) -> Math.max(currentOffset, logStartOffset));
        rebuildLeaderEpochCheckpoint();
        recomputeHighWatermark();
        persistCheckpoint();
    }

    /**
     * 当前分区日志是否为空。
     */
    public synchronized boolean isEmpty() {
        return segments.size() == 1 && activeSegment().isEmpty();
    }

    /**
     * 当前高水位。
     */
    public synchronized long highWatermark() {
        return highWatermark;
    }

    /**
     * 当前 leader epoch。
     */
    public synchronized int leaderEpoch() {
        return leaderEpoch;
    }

    /**
     * 当前 leaderId。
     */
    public synchronized int leaderId() {
        return leaderId;
    }

    /**
     * 当前副本集合。
     */
    public synchronized List<Integer> replicaNodes() {
        return List.copyOf(replicaNodes);
    }

    /**
     * 当前 ISR 集合。
     */
    public synchronized List<Integer> isrNodes() {
        return List.copyOf(isr);
    }

    /**
     * 返回指定副本最近一次上报的复制进度。
     */
    public synchronized long replicaEndOffset(int brokerId) {
        return replicaEndOffsets.getOrDefault(brokerId, logStartOffset);
    }

    /**
     * 返回全部副本复制进度快照。
     */
    public synchronized Map<Integer, Long> replicaEndOffsets() {
        return Map.copyOf(replicaEndOffsets);
    }

    /**
     * 当前 log end offset。
     */
    public synchronized long logEndOffset() {
        return logEndOffset;
    }

    /**
     * 当前 log start offset。
     */
    public synchronized long logStartOffset() {
        return logStartOffset;
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public Path partitionDir() {
        return partitionDir;
    }

    /**
     * 初始化目录、段和 checkpoint。
     */
    private void initialize() {
        try {
            Files.createDirectories(partitionDir);
            loadSegments();
            LogCheckpointState checkpointState = checkpoint.load();
            logEndOffset = activeSegment().nextOffset();
            logStartOffset =
                    Math.max(
                            segments.isEmpty() ? 0 : segments.get(0).baseOffset(),
                            checkpointState.logStartOffset());
            highWatermark = Math.min(checkpointState.highWatermark(), logEndOffset);
            leaderEpoch = Math.max(0, checkpointState.leaderEpoch());
            rebuildLeaderEpochCheckpoint();
            replicaNodes.add(LOCAL_BROKER_ID);
            isr.add(LOCAL_BROKER_ID);
            replicaEndOffsets.put(LOCAL_BROKER_ID, logEndOffset);
            recomputeHighWatermark();
            persistCheckpoint();
        } catch (IOException exception) {
            throw new IllegalStateException(
                    "Failed to initialize unified log for " + topicPartition, exception);
        }
    }

    /**
     * 加载已有段。
     */
    private void loadSegments() throws IOException {
        segments.clear();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(partitionDir, "*" + LOG_EXTENSION)) {
            for (Path path : stream) {
                long baseOffset = parseBaseOffset(path);
                segments.add(
                        new LogSegment(path, baseOffset, segmentBytes, indexIntervalBytes));
            }
        }
        segments.sort(Comparator.comparingLong(LogSegment::baseOffset));
        if (segments.isEmpty()) {
            segments.add(createSegment(0));
        }
    }

    /**
     * 创建新段并切换为 active。
     */
    private LogSegment roll(long baseOffset) {
        LogSegment segment = createSegment(baseOffset);
        segments.add(segment);
        segments.sort(Comparator.comparingLong(LogSegment::baseOffset));
        return segment;
    }

    /**
     * 创建具体段实例。
     */
    private LogSegment createSegment(long baseOffset) {
        String fileName = String.format("%020d%s", baseOffset, LOG_EXTENSION);
        return new LogSegment(
                partitionDir.resolve(fileName), baseOffset, segmentBytes, indexIntervalBytes);
    }

    /**
     * 返回 active 段。
     */
    private LogSegment activeSegment() {
        return segments.get(segments.size() - 1);
    }

    /**
     * 根据保留策略删除最早段。
     */
    private void maybeApplyRetention(long nowMs) {
        boolean changed = false;
        while (segments.size() > retentionSegments && segments.size() > 1) {
            LogSegment segment = segments.get(0);
            segments.remove(0);
            segment.deleteFiles();
            logStartOffset = segments.get(0).baseOffset();
            changed = true;
        }
        while (segments.size() > 1 && totalSizeInBytes() > retentionBytes) {
            LogSegment segment = segments.get(0);
            segments.remove(0);
            segment.deleteFiles();
            logStartOffset = segments.get(0).baseOffset();
            changed = true;
        }
        long cutoff = nowMs - retentionMs;
        while (segments.size() > 1 && segments.get(0).maxTimestamp() > 0 && segments.get(0).maxTimestamp() < cutoff) {
            LogSegment segment = segments.get(0);
            segments.remove(0);
            segment.deleteFiles();
            logStartOffset = segments.get(0).baseOffset();
            changed = true;
        }
        replicaEndOffsets.replaceAll((ignored, currentOffset) -> Math.max(currentOffset, logStartOffset));
        if (changed) {
            rebuildLeaderEpochCheckpoint();
        }
    }

    /**
     * 返回当前总字节数。
     */
    private long totalSizeInBytes() {
        long total = 0;
        for (LogSegment segment : segments) {
            total += segment.sizeInBytes();
        }
        return total;
    }

    /**
     * 按 ISR 推进高水位。
     */
    private void recomputeHighWatermark() {
        long candidate = logEndOffset;
        for (Integer replicaId : isr) {
            long replicaOffset = replicaEndOffsets.getOrDefault(replicaId, 0L);
            candidate = Math.min(candidate, replicaOffset);
        }
        highWatermark = Math.max(logStartOffset, candidate);
    }

    /**
     * 按当前段内容重建 leader epoch 检查点。
     */
    private void rebuildLeaderEpochCheckpoint() {
        java.util.NavigableMap<Integer, Long> epochOffsets = new java.util.TreeMap<>();
        for (LogSegment segment : segments) {
            for (Map.Entry<Integer, Long> entry : segment.epochStartOffsets().entrySet()) {
                epochOffsets.putIfAbsent(entry.getKey(), entry.getValue());
            }
        }
        if (epochOffsets.isEmpty()) {
            epochOffsets.put(leaderEpoch, logStartOffset);
        }
        leaderEpochCheckpoint.rebuild(epochOffsets);
    }

    /**
     * 从文件名解析 baseOffset。
     */
    private long parseBaseOffset(Path path) {
        String fileName = path.getFileName().toString();
        int dot = fileName.indexOf('.');
        String prefix = dot >= 0 ? fileName.substring(0, dot) : fileName;
        return Long.parseLong(prefix);
    }

    /**
     * 持久化 checkpoint。
     */
    private void persistCheckpoint() {
        checkpoint.persist(
                new LogCheckpointState(
                        highWatermark, logEndOffset, logEndOffset, logStartOffset, leaderEpoch));
    }

    @Override
    public synchronized void close() {
        RuntimeException firstException = null;
        for (LogSegment segment : segments) {
            try {
                segment.close();
            } catch (RuntimeException exception) {
                if (firstException == null) {
                    firstException = exception;
                }
            }
        }
        if (firstException != null) {
            throw firstException;
        }
    }

    /**
     * 删除当前分区的全部本地日志文件与目录。
     */
    public synchronized void deleteAllData() {
        RuntimeException firstException = null;
        for (LogSegment segment : new ArrayList<>(segments)) {
            try {
                segment.deleteFiles();
            } catch (RuntimeException exception) {
                if (firstException == null) {
                    firstException = exception;
                }
            }
        }
        segments.clear();
        try {
            Files.deleteIfExists(partitionDir.resolve(CHECKPOINT_FILE_NAME));
            Files.deleteIfExists(partitionDir.resolve(LEADER_EPOCH_CHECKPOINT_FILE_NAME));
            Files.deleteIfExists(partitionDir);
        } catch (IOException exception) {
            if (firstException == null) {
                firstException =
                        new IllegalStateException(
                                "Failed to delete unified log directory " + partitionDir, exception);
            }
        }
        if (firstException != null) {
            throw firstException;
        }
    }
}
