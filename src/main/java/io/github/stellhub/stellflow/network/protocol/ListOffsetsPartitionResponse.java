package io.github.stellhub.stellflow.network.protocol;

/**
 * ListOffsets 分区响应。
 */
public record ListOffsetsPartitionResponse(
        int partition,
        ErrorCode errorCode,
        int leaderEpoch,
        long timestamp,
        long offset,
        java.util.List<Long> offsets) {}
