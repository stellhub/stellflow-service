package io.github.stellhub.stellflow.server.api;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 最小内存消息存储，用于协议冒烟测试。
 */
public class InMemoryMessageStore {

    private final Map<String, Map<Integer, PartitionLog>> topics = new ConcurrentHashMap<>();

    /**
     * 追加分区记录。
     */
    public AppendResult append(String topic, int partition, byte[] records) {
        PartitionLog partitionLog =
                topics.computeIfAbsent(topic, ignored -> new ConcurrentHashMap<>())
                        .computeIfAbsent(partition, ignored -> new PartitionLog());
        return partitionLog.append(records);
    }

    /**
     * 拉取分区记录。
     */
    public FetchResult fetch(String topic, int partition, long fetchOffset, int partitionMaxBytes) {
        Map<Integer, PartitionLog> partitions = topics.get(topic);
        if (partitions == null) {
            return null;
        }
        PartitionLog partitionLog = partitions.get(partition);
        if (partitionLog == null) {
            return null;
        }
        return partitionLog.fetch(fetchOffset, partitionMaxBytes);
    }

    /**
     * 判断 topic 是否存在。
     */
    public boolean containsTopic(String topic) {
        return topics.containsKey(topic);
    }

    /**
     * 返回全部 topic。
     */
    public Set<String> topicNames() {
        return topics.keySet();
    }

    /**
     * 返回某个 topic 的分区编号。
     */
    public List<Integer> partitions(String topic) {
        Map<Integer, PartitionLog> partitions = topics.get(topic);
        if (partitions == null) {
            return List.of();
        }
        List<Integer> values = new ArrayList<>(partitions.keySet());
        Collections.sort(values);
        return values;
    }

    /**
     * 返回指定分区的高水位。
     */
    public long highWatermark(String topic, int partition) {
        Map<Integer, PartitionLog> partitions = topics.get(topic);
        if (partitions == null || partitions.get(partition) == null) {
            return 0;
        }
        return partitions.get(partition).nextOffset();
    }

    /**
     * 返回当前存储中分区的 leader epoch 占位值。
     */
    public int leaderEpoch(String topic, int partition) {
        return 0;
    }

    /**
     * 追加结果。
     */
    public record AppendResult(long baseOffset, long highWatermark, int leaderEpoch) {}

    /**
     * 拉取结果。
     */
    public record FetchResult(
            long highWatermark, long logStartOffset, long lastStableOffset, byte[] records) {}

    /**
     * 分区级内存日志。
     */
    private static final class PartitionLog {

        private final List<Entry> entries = new ArrayList<>();
        private long nextOffset;

        /**
         * 顺序追加一条 batch。
         */
        synchronized AppendResult append(byte[] records) {
            long baseOffset = nextOffset;
            entries.add(new Entry(baseOffset, records == null ? new byte[0] : records.clone()));
            nextOffset++;
            return new AppendResult(baseOffset, nextOffset, 0);
        }

        /**
         * 按 offset 拉取批次。
         */
        synchronized FetchResult fetch(long fetchOffset, int partitionMaxBytes) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            for (Entry entry : entries) {
                if (entry.baseOffset() < fetchOffset) {
                    continue;
                }
                if (outputStream.size() + entry.records().length > partitionMaxBytes
                        && outputStream.size() > 0) {
                    break;
                }
                try {
                    outputStream.write(entry.records());
                } catch (IOException exception) {
                    throw new UncheckedIOException(exception);
                }
            }
            return new FetchResult(nextOffset, 0, nextOffset, outputStream.toByteArray());
        }

        /**
         * 返回下一个 offset。
         */
        synchronized long nextOffset() {
            return nextOffset;
        }
    }

    /**
     * 单条内存记录。
     */
    private record Entry(long baseOffset, byte[] records) {}
}
