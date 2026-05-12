package io.github.stellhub.stellflow.coordinator;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Offset 存储抽象。
 */
public class OffsetStore {

    private final ConcurrentMap<OffsetKey, OffsetAndMetadata> offsets = new ConcurrentHashMap<>();

    /**
     * 提交消费位点。
     */
    public void commit(String groupId, String topic, int partition, long offset, String metadata) {
        offsets.put(
                new OffsetKey(groupId, topic, partition),
                new OffsetAndMetadata(offset, metadata, System.currentTimeMillis()));
    }

    /**
     * 查询消费位点。
     */
    public Optional<OffsetAndMetadata> fetch(String groupId, String topic, int partition) {
        return Optional.ofNullable(offsets.get(new OffsetKey(groupId, topic, partition)));
    }
}
