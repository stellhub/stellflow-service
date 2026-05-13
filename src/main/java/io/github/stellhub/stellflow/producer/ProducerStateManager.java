package io.github.stellhub.stellflow.producer;

import io.github.stellhub.stellflow.network.protocol.CompressionType;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.network.protocol.RecordBatchFormat;
import io.github.stellhub.stellflow.network.protocol.RecordBatchInfo;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Producer 幂等状态管理器。
 */
public class ProducerStateManager {

    private final AtomicLong producerIdGenerator = new AtomicLong(1);
    private final Map<String, ProducerState> states = new ConcurrentHashMap<>();
    private final Map<Long, ProducerRuntimeState> idempotentStates = new ConcurrentHashMap<>();

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
                                producerIdGenerator.getAndIncrement(),
                                (short) 0,
                                System.currentTimeMillis());
                    }
                    return new ProducerState(
                            current.producerId(), current.producerEpoch(), System.currentTimeMillis());
                });
    }

    /**
     * 校验幂等 producer 序列号并判断是否需要真正追加。
     */
    public ProducerAppendDecision validateAppend(
            String producerKey, String topic, int partition, byte[] records) {
        RecordBatchInfo batchInfo;
        try {
            Optional<RecordBatchInfo> parsed = RecordBatchFormat.tryReadInfo(records);
            batchInfo =
                    parsed.orElseGet(
                            () ->
                                    new RecordBatchInfo(
                                            false,
                                            -1,
                                            (short) -1,
                                            -1,
                                            0,
                                            CompressionType.NONE,
                                            records == null ? 0 : records.length,
                                            records == null ? 0 : records.length,
                                            RecordBatchFormat.fingerprint(records)));
        } catch (RuntimeException exception) {
            return ProducerAppendDecision.reject(ErrorCode.INVALID_RECORD, null);
        }
        if (!batchInfo.idempotent()) {
            getOrCreate(producerKey);
            return ProducerAppendDecision.append(batchInfo);
        }
        ProducerRuntimeState runtimeState =
                idempotentStates.computeIfAbsent(
                        batchInfo.producerId(),
                        ignored -> new ProducerRuntimeState(batchInfo.producerEpoch()));
        synchronized (runtimeState) {
            if (batchInfo.producerEpoch() < runtimeState.producerEpoch) {
                return ProducerAppendDecision.reject(ErrorCode.FENCED_INSTANCE_ID, batchInfo);
            }
            if (batchInfo.producerEpoch() > runtimeState.producerEpoch) {
                runtimeState.producerEpoch = batchInfo.producerEpoch();
                runtimeState.partitions.clear();
                runtimeState.inTransaction = false;
            }
            if (producerKey != null && !producerKey.isBlank()) {
                if (runtimeState.transactionalId != null
                        && !runtimeState.transactionalId.equals(producerKey)) {
                    return ProducerAppendDecision.reject(ErrorCode.CONCURRENT_TRANSACTIONS, batchInfo);
                }
                runtimeState.transactionalId = producerKey;
                runtimeState.inTransaction = true;
            }
            PartitionSequenceState sequenceState =
                    runtimeState.partitions.computeIfAbsent(
                            partitionKey(topic, partition), ignored -> new PartitionSequenceState());
            BatchAppendSnapshot previous = sequenceState.completed.get(batchInfo.baseSequence());
            if (previous != null) {
                if (previous.recordCount == batchInfo.recordCount()
                        && previous.fingerprint == batchInfo.fingerprint()) {
                    return ProducerAppendDecision.duplicate(previous.baseOffset, batchInfo);
                }
                return ProducerAppendDecision.reject(ErrorCode.OUT_OF_ORDER_SEQUENCE, batchInfo);
            }
            if (batchInfo.baseSequence() != sequenceState.nextSequence) {
                return ProducerAppendDecision.reject(ErrorCode.OUT_OF_ORDER_SEQUENCE, batchInfo);
            }
            return ProducerAppendDecision.append(batchInfo);
        }
    }

    /**
     * 记录成功追加的幂等批次。
     */
    public void recordAppendSuccess(
            RecordBatchInfo batchInfo, String topic, int partition, long baseOffset) {
        if (batchInfo == null || !batchInfo.idempotent()) {
            return;
        }
        ProducerRuntimeState runtimeState = idempotentStates.get(batchInfo.producerId());
        if (runtimeState == null) {
            return;
        }
        synchronized (runtimeState) {
            PartitionSequenceState sequenceState =
                    runtimeState.partitions.computeIfAbsent(
                            partitionKey(topic, partition), ignored -> new PartitionSequenceState());
            sequenceState.nextSequence = batchInfo.baseSequence() + batchInfo.recordCount();
            sequenceState.completed.put(
                    batchInfo.baseSequence(),
                    new BatchAppendSnapshot(
                            batchInfo.recordCount(), batchInfo.fingerprint(), baseOffset));
        }
    }

    /**
     * 显式开启事务。
     */
    public void beginTransaction(String transactionalId, long producerId, short producerEpoch) {
        ProducerRuntimeState runtimeState =
                idempotentStates.computeIfAbsent(
                        producerId, ignored -> new ProducerRuntimeState(producerEpoch));
        synchronized (runtimeState) {
            if (runtimeState.inTransaction
                    && runtimeState.transactionalId != null
                    && !runtimeState.transactionalId.equals(transactionalId)) {
                throw new IllegalStateException("Producer already has an active transaction");
            }
            runtimeState.transactionalId = transactionalId;
            runtimeState.producerEpoch = producerEpoch;
            runtimeState.inTransaction = true;
        }
    }

    /**
     * 提交事务状态。
     */
    public void commitTransaction(long producerId) {
        ProducerRuntimeState runtimeState = idempotentStates.get(producerId);
        if (runtimeState == null) {
            return;
        }
        synchronized (runtimeState) {
            runtimeState.inTransaction = false;
        }
    }

    /**
     * 中止事务状态。
     */
    public void abortTransaction(long producerId) {
        ProducerRuntimeState runtimeState = idempotentStates.get(producerId);
        if (runtimeState == null) {
            return;
        }
        synchronized (runtimeState) {
            runtimeState.inTransaction = false;
        }
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
                                producerIdGenerator.getAndIncrement(),
                                (short) 0,
                                System.currentTimeMillis());
                    }
                    return new ProducerState(
                            current.producerId(),
                            (short) (current.producerEpoch() + 1),
                            System.currentTimeMillis());
                });
    }

    private String partitionKey(String topic, int partition) {
        return topic + ":" + partition;
    }

    private static final class ProducerRuntimeState {
        private final Map<String, PartitionSequenceState> partitions = new ConcurrentHashMap<>();
        private short producerEpoch;
        private String transactionalId;
        private boolean inTransaction;

        private ProducerRuntimeState(short producerEpoch) {
            this.producerEpoch = producerEpoch;
        }
    }

    private static final class PartitionSequenceState {
        private final Map<Integer, BatchAppendSnapshot> completed = new ConcurrentHashMap<>();
        private int nextSequence;
    }

    private record BatchAppendSnapshot(int recordCount, int fingerprint, long baseOffset) {}
}
