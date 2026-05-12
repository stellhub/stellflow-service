package io.github.stellhub.stellflow.server.runtime;

import io.github.stellhub.stellflow.network.protocol.ErrorCode;

/**
 * 分区追加结果。
 */
public record PartitionAppendResult(
        ErrorCode errorCode,
        long baseOffset,
        long logAppendTimeMs,
        long logStartOffset,
        int leaderEpoch) {}
