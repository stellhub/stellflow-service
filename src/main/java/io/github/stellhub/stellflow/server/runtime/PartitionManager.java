package io.github.stellhub.stellflow.server.runtime;

import io.github.stellhub.stellflow.metadata.PartitionMetadata;
import io.github.stellhub.stellflow.metadata.PartitionRole;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.storage.log.LogAppendResult;
import io.github.stellhub.stellflow.storage.log.LogManager;
import io.github.stellhub.stellflow.storage.log.LogReadResult;
import io.github.stellhub.stellflow.storage.log.ReplicaLogReadResult;
import io.github.stellhub.stellflow.storage.log.TimestampOffsetResult;
import java.util.List;

/**
 * 单个分区运行时管理器。
 */
public class PartitionManager {

    private static final long LATEST_TIMESTAMP = -1L;
    private static final long EARLIEST_TIMESTAMP = -2L;

    private final LogManager logManager;
    private final PartitionMetadata metadata;

    public PartitionManager(LogManager logManager, PartitionMetadata metadata) {
        this.logManager = logManager;
        this.metadata = metadata;
    }

    /**
     * 作为 leader 追加消息。
     */
    public PartitionAppendResult appendAsLeader(byte[] records) {
        return appendAsLeader(records, (short) 1, 0);
    }

    /**
     * 作为 leader 按 acks 语义追加消息。
     */
    public PartitionAppendResult appendAsLeader(byte[] records, short acks, int timeoutMs) {
        if (metadata.localRole() != PartitionRole.LEADER) {
            return new PartitionAppendResult(
                    ErrorCode.NOT_LEADER_OR_FOLLOWER,
                    -1,
                    0,
                    logStartOffset(),
                    metadata.leaderEpoch(),
                    0,
                    0);
        }
        if (records == null || records.length == 0) {
            return new PartitionAppendResult(
                    ErrorCode.INVALID_RECORD,
                    -1,
                    0,
                    logStartOffset(),
                    metadata.leaderEpoch(),
                    logManager.highWatermark(metadata.topic(), metadata.partition()),
                    logManager.logEndOffset(metadata.topic(), metadata.partition()));
        }
        LogAppendResult result = logManager.append(metadata.topic(), metadata.partition(), records);
        long requiredOffset = result.logEndOffset();
        if (acks == -1 && !waitForHighWatermark(requiredOffset, timeoutMs)) {
            return new PartitionAppendResult(
                    ErrorCode.BROKER_NOT_AVAILABLE,
                    result.baseOffset(),
                    0,
                    logManager.logStartOffset(metadata.topic(), metadata.partition()),
                    result.leaderEpoch(),
                    logManager.highWatermark(metadata.topic(), metadata.partition()),
                    result.logEndOffset());
        }
        return new PartitionAppendResult(
                ErrorCode.NONE,
                result.baseOffset(),
                0,
                logManager.logStartOffset(metadata.topic(), metadata.partition()),
                result.leaderEpoch(),
                logManager.highWatermark(metadata.topic(), metadata.partition()),
                result.logEndOffset());
    }

    /**
     * 处理普通 consumer fetch。
     */
    public PartitionReadResult readConsumer(long fetchOffset, int maxBytes) {
        if (metadata.localRole() != PartitionRole.LEADER) {
            return emptyRead(ErrorCode.NOT_LEADER_OR_FOLLOWER);
        }
        LogReadResult result = logManager.read(metadata.topic(), metadata.partition(), fetchOffset, maxBytes);
        if (result == null) {
            return emptyRead(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION);
        }
        return new PartitionReadResult(
                ErrorCode.NONE,
                result.highWatermark(),
                result.logStartOffset(),
                result.lastStableOffset(),
                result.nextFetchOffset(),
                result.records(),
                List.of());
    }

    /**
     * 处理 follower replica fetch。
     */
    public PartitionReadResult readReplica(int replicaId, long fetchOffset, int maxBytes) {
        if (metadata.localRole() != PartitionRole.LEADER) {
            return emptyRead(ErrorCode.NOT_LEADER_OR_FOLLOWER);
        }
        ReplicaLogReadResult result =
                logManager.readReplica(metadata.topic(), metadata.partition(), fetchOffset, maxBytes);
        if (result == null) {
            return emptyRead(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION);
        }
        logManager.updateReplicaFetchOffset(
                metadata.topic(), metadata.partition(), replicaId, result.nextFetchOffset());
        return new PartitionReadResult(
                ErrorCode.NONE,
                result.highWatermark(),
                result.logStartOffset(),
                result.lastStableOffset(),
                result.nextFetchOffset(),
                new byte[0],
                result.entries());
    }

    /**
     * 查询分区 offset。
     */
    public PartitionOffsetResult listOffsets(long timestamp, int currentLeaderEpoch, int maxNumOffsets) {
        int leaderEpoch = metadata.leaderEpoch();
        if (currentLeaderEpoch >= 0 && currentLeaderEpoch != leaderEpoch) {
            return new PartitionOffsetResult(
                    ErrorCode.NOT_LEADER_OR_FOLLOWER, leaderEpoch, 0, 0, List.of());
        }
        if (maxNumOffsets <= 0) {
            return new PartitionOffsetResult(ErrorCode.INVALID_REQUEST, leaderEpoch, 0, 0, List.of());
        }
        if (timestamp == LATEST_TIMESTAMP) {
            long latestOffset = logManager.logEndOffset(metadata.topic(), metadata.partition());
            return new PartitionOffsetResult(
                    ErrorCode.NONE,
                    leaderEpoch,
                    System.currentTimeMillis(),
                    latestOffset,
                    List.of(latestOffset));
        }
        if (timestamp == EARLIEST_TIMESTAMP) {
            long earliestOffset = logManager.logStartOffset(metadata.topic(), metadata.partition());
            return new PartitionOffsetResult(
                    ErrorCode.NONE,
                    leaderEpoch,
                    0,
                    earliestOffset,
                    logManager.listOffsetsForTimestamp(
                            metadata.topic(), metadata.partition(), timestamp, maxNumOffsets));
        }
        TimestampOffsetResult result =
                logManager.findOffsetByTimestamp(metadata.topic(), metadata.partition(), timestamp);
        List<Long> offsets =
                logManager.listOffsetsForTimestamp(
                        metadata.topic(), metadata.partition(), timestamp, maxNumOffsets);
        if (offsets.isEmpty()) {
            return new PartitionOffsetResult(
                    ErrorCode.OFFSET_OUT_OF_RANGE,
                    leaderEpoch,
                    timestamp,
                    logManager.logEndOffset(metadata.topic(), metadata.partition()),
                    List.of());
        }
        return new PartitionOffsetResult(
                ErrorCode.NONE, leaderEpoch, result.timestamp(), result.offset(), offsets);
    }

    private PartitionReadResult emptyRead(ErrorCode errorCode) {
        return new PartitionReadResult(errorCode, 0, 0, 0, 0, new byte[0], List.of());
    }

    private long logStartOffset() {
        return logManager.logStartOffset(metadata.topic(), metadata.partition());
    }

    private boolean waitForHighWatermark(long requiredOffset, int timeoutMs) {
        long deadlineMs = System.currentTimeMillis() + Math.max(timeoutMs, 0);
        while (logManager.highWatermark(metadata.topic(), metadata.partition()) < requiredOffset) {
            if (timeoutMs <= 0 || System.currentTimeMillis() >= deadlineMs) {
                return false;
            }
            try {
                Thread.sleep(Math.min(10, Math.max(1, deadlineMs - System.currentTimeMillis())));
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return true;
    }
}
