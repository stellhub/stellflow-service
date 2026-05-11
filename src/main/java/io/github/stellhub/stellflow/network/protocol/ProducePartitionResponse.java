package io.github.stellhub.stellflow.network.protocol;

/**
 * Produce 分区响应。
 */
public record ProducePartitionResponse(
        int partition,
        ErrorCode errorCode,
        long baseOffset,
        int currentLeaderEpoch,
        long logAppendTimeMs,
        long logStartOffset) {}
