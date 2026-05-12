package io.github.stellhub.stellflow.storage.log;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 日志管理器。
 */
public class LogManager implements AutoCloseable {

    private final LogStorageConfig storageConfig;
    private final Path rootDir;
    private final Map<TopicPartition, UnifiedLog> logs = new ConcurrentHashMap<>();

    public LogManager(Path rootDir) {
        this(defaultConfigFor(rootDir));
    }

    public LogManager(LogStorageConfig storageConfig) {
        this.storageConfig = storageConfig;
        this.rootDir = storageConfig.getRootDir();
        try {
            Files.createDirectories(rootDir);
            loadExistingLogs();
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to initialize log root dir " + rootDir, exception);
        }
    }

    /**
     * 追加分区数据。
     */
    public LogAppendResult append(String topic, int partition, byte[] records) {
        return getOrCreateLog(new TopicPartition(topic, partition)).appendAsLeader(records);
    }

    /**
     * 读取分区数据。
     */
    public LogReadResult read(String topic, int partition, long fetchOffset, int maxBytes) {
        UnifiedLog log = logs.get(new TopicPartition(topic, partition));
        if (log == null) {
            return null;
        }
        return log.read(fetchOffset, maxBytes);
    }

    /**
     * 读取副本同步数据。
     */
    public ReplicaLogReadResult readReplica(String topic, int partition, long fetchOffset, int maxBytes) {
        UnifiedLog log = logs.get(new TopicPartition(topic, partition));
        if (log == null) {
            return null;
        }
        return log.readReplica(fetchOffset, maxBytes);
    }

    /**
     * 根据时间戳查找 offset。
     */
    public TimestampOffsetResult findOffsetByTimestamp(String topic, int partition, long timestamp) {
        UnifiedLog log = logs.get(new TopicPartition(topic, partition));
        if (log == null) {
            return new TimestampOffsetResult(0, 0);
        }
        return log.findOffsetByTimestamp(timestamp);
    }

    /**
     * 根据时间戳列出最多 N 个 offset。
     */
    public List<Long> listOffsetsForTimestamp(
            String topic, int partition, long timestamp, int maxNumOffsets) {
        UnifiedLog log = logs.get(new TopicPartition(topic, partition));
        if (log == null) {
            return List.of();
        }
        return log.listOffsetsForTimestamp(timestamp, maxNumOffsets);
    }

    /**
     * 推进分区 leader epoch。
     */
    public void updateLeaderEpoch(String topic, int partition, int leaderEpoch) {
        getOrCreateLog(new TopicPartition(topic, partition)).updateLeaderEpoch(leaderEpoch);
    }

    /**
     * 设置分区副本拓扑。
     */
    public void updateReplicaTopology(
            String topic,
            int partition,
            int leaderId,
            List<Integer> replicaNodes,
            List<Integer> isr) {
        getOrCreateLog(new TopicPartition(topic, partition))
                .updateReplicaTopology(leaderId, replicaNodes, isr);
    }

    /**
     * 更新 follower 已复制到的 offset。
     */
    public void updateReplicaFetchOffset(
            String topic, int partition, int followerBrokerId, long replicaEndOffset) {
        getOrCreateLog(new TopicPartition(topic, partition))
                .updateReplicaFetchOffset(followerBrokerId, replicaEndOffset);
    }

    /**
     * 追加 follower 同步下来的数据。
     */
    public void appendReplicaEntries(
            String topic, int partition, List<ReplicaLogEntry> replicaEntries, int leaderEpoch) {
        getOrCreateLog(new TopicPartition(topic, partition))
                .appendReplicaEntries(replicaEntries, leaderEpoch);
    }

    /**
     * 截断分区日志。
     */
    public void truncateTo(String topic, int partition, long offset) {
        getOrCreateLog(new TopicPartition(topic, partition)).truncateTo(offset);
    }

    /**
     * 按 epoch 分叉点截断分区日志。
     */
    public void truncateToLeaderEpoch(String topic, int partition, int epoch) {
        getOrCreateLog(new TopicPartition(topic, partition)).truncateToLeaderEpoch(epoch);
    }

    /**
     * 删除早于指定 offset 的完整 segment。
     */
    public void deleteSegmentsBeforeOffset(String topic, int partition, long offset) {
        getOrCreateLog(new TopicPartition(topic, partition)).deleteSegmentsBeforeOffset(offset);
    }

    /**
     * 返回当前 logEndOffset。
     */
    public long logEndOffset(String topic, int partition) {
        UnifiedLog log = logs.get(new TopicPartition(topic, partition));
        if (log == null) {
            return 0;
        }
        return log.logEndOffset();
    }

    /**
     * 判断 topic 是否存在。
     */
    public boolean containsTopic(String topic) {
        return logs.keySet().stream().anyMatch(topicPartition -> topicPartition.topic().equals(topic));
    }

    /**
     * 返回全部 topic 名。
     */
    public Set<String> topicNames() {
        Set<String> values = new TreeSet<>();
        for (TopicPartition topicPartition : logs.keySet()) {
            values.add(topicPartition.topic());
        }
        return values;
    }

    /**
     * 返回某个 topic 的分区集合。
     */
    public List<Integer> partitions(String topic) {
        List<Integer> values = new ArrayList<>();
        for (TopicPartition topicPartition : logs.keySet()) {
            if (topicPartition.topic().equals(topic)) {
                values.add(topicPartition.partition());
            }
        }
        values.sort(Integer::compareTo);
        return values;
    }

    /**
     * 返回当前 leader epoch。
     */
    public int leaderEpoch(String topic, int partition) {
        UnifiedLog log = logs.get(new TopicPartition(topic, partition));
        if (log == null) {
            return 0;
        }
        return log.leaderEpoch();
    }

    /**
     * 返回当前高水位。
     */
    public long highWatermark(String topic, int partition) {
        UnifiedLog log = logs.get(new TopicPartition(topic, partition));
        if (log == null) {
            return 0;
        }
        return log.highWatermark();
    }

    /**
     * 返回当前 logStartOffset。
     */
    public long logStartOffset(String topic, int partition) {
        UnifiedLog log = logs.get(new TopicPartition(topic, partition));
        if (log == null) {
            return 0;
        }
        return log.logStartOffset();
    }

    /**
     * 返回分区 leaderId。
     */
    public int leaderId(String topic, int partition) {
        UnifiedLog log = logs.get(new TopicPartition(topic, partition));
        if (log == null) {
            return 0;
        }
        return log.leaderId();
    }

    /**
     * 返回分区 replica 列表。
     */
    public List<Integer> replicaNodes(String topic, int partition) {
        UnifiedLog log = logs.get(new TopicPartition(topic, partition));
        if (log == null) {
            return List.of(0);
        }
        return log.replicaNodes();
    }

    /**
     * 返回分区 ISR 列表。
     */
    public List<Integer> isrNodes(String topic, int partition) {
        UnifiedLog log = logs.get(new TopicPartition(topic, partition));
        if (log == null) {
            return List.of(0);
        }
        return log.isrNodes();
    }

    /**
     * 返回指定副本最近一次复制进度。
     */
    public long replicaEndOffset(String topic, int partition, int brokerId) {
        UnifiedLog log = logs.get(new TopicPartition(topic, partition));
        if (log == null) {
            return 0;
        }
        return log.replicaEndOffset(brokerId);
    }

    /**
     * 删除单个分区的本地存储。
     */
    public void deletePartition(String topic, int partition) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        UnifiedLog log = logs.remove(topicPartition);
        if (log != null) {
            log.deleteAllData();
        }
    }

    /**
     * 删除整个 topic 的本地存储。
     */
    public void deleteTopic(String topic) {
        List<Integer> partitions = partitions(topic);
        for (Integer partition : partitions) {
            deletePartition(topic, partition);
        }
    }

    /**
     * 判断指定分区是否存在本地日志。
     */
    public boolean containsPartition(String topic, int partition) {
        return logs.containsKey(new TopicPartition(topic, partition));
    }

    /**
     * 获取或创建日志。
     */
    private UnifiedLog getOrCreateLog(TopicPartition topicPartition) {
        return logs.computeIfAbsent(
                topicPartition,
                ignored ->
                        new UnifiedLog(
                                topicPartition,
                                rootDir.resolve(
                                        topicPartition.topic() + "-" + topicPartition.partition()),
                                storageConfig.getSegmentBytes(),
                                storageConfig.getIndexIntervalBytes(),
                                storageConfig.getRetentionSegments(),
                                storageConfig.getRetentionMs(),
                                storageConfig.getRetentionBytes()));
    }

    /**
     * 启动时扫描已有分区目录。
     */
    private void loadExistingLogs() throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(rootDir)) {
            for (Path entry : stream) {
                if (!Files.isDirectory(entry)) {
                    continue;
                }
                TopicPartition topicPartition = parseTopicPartition(entry.getFileName().toString());
                if (topicPartition == null) {
                    continue;
                }
                logs.put(
                        topicPartition,
                        new UnifiedLog(
                                topicPartition,
                                entry,
                                storageConfig.getSegmentBytes(),
                                storageConfig.getIndexIntervalBytes(),
                                storageConfig.getRetentionSegments(),
                                storageConfig.getRetentionMs(),
                                storageConfig.getRetentionBytes()));
            }
        }
    }

    /**
     * 从目录名反解 topic-partition。
     */
    private TopicPartition parseTopicPartition(String directoryName) {
        int separator = directoryName.lastIndexOf('-');
        if (separator <= 0 || separator == directoryName.length() - 1) {
            return null;
        }
        String topic = directoryName.substring(0, separator);
        String partitionPart = directoryName.substring(separator + 1);
        try {
            int partition = Integer.parseInt(partitionPart);
            return new TopicPartition(topic, partition);
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    @Override
    public void close() {
        for (UnifiedLog log : logs.values()) {
            log.close();
        }
    }

    /**
     * 使用全局默认项构造日志配置。
     */
    private static LogStorageConfig defaultConfigFor(Path rootDir) {
        LogStorageConfig defaults = LogStorageConfig.load();
        return LogStorageConfig.builder()
                .rootDir(rootDir)
                .segmentBytes(defaults.getSegmentBytes())
                .indexIntervalBytes(defaults.getIndexIntervalBytes())
                .retentionSegments(defaults.getRetentionSegments())
                .retentionMs(defaults.getRetentionMs())
                .retentionBytes(defaults.getRetentionBytes())
                .build();
    }
}
