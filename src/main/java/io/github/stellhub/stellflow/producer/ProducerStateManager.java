package io.github.stellhub.stellflow.producer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Producer 幂等状态管理器。
 */
public class ProducerStateManager {

    private final AtomicLong producerIdGenerator = new AtomicLong(1);
    private final Map<String, ProducerState> states = new ConcurrentHashMap<>();

    /**
     * 获取或创建 producer 状态。
     */
    public ProducerState getOrCreate(String producerKey) {
        String key = producerKey == null || producerKey.isBlank() ? "anonymous" : producerKey;
        return states.compute(
                key,
                (ignored, current) -> {
                    if (current == null) {
                        return new ProducerState(
                                producerIdGenerator.getAndIncrement(), (short) 0, System.currentTimeMillis());
                    }
                    return new ProducerState(
                            current.producerId(), current.producerEpoch(), System.currentTimeMillis());
                });
    }

    /**
     * 提升 producer epoch，用于 fencing 旧实例。
     */
    public ProducerState bumpEpoch(String producerKey) {
        String key = producerKey == null || producerKey.isBlank() ? "anonymous" : producerKey;
        return states.compute(
                key,
                (ignored, current) -> {
                    if (current == null) {
                        return new ProducerState(
                                producerIdGenerator.getAndIncrement(), (short) 0, System.currentTimeMillis());
                    }
                    return new ProducerState(
                            current.producerId(),
                            (short) (current.producerEpoch() + 1),
                            System.currentTimeMillis());
                });
    }
}
