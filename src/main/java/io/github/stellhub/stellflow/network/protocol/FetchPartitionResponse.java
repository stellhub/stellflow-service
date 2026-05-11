package io.github.stellhub.stellflow.network.protocol;

import java.util.List;

/**
 * Fetch 分区响应。
 */
public record FetchPartitionResponse(
        int partition,
        ErrorCode errorCode,
        long highWatermark,
        long logStartOffset,
        long lastStableOffset,
        List<AbortedTransaction> abortedTransactions,
        byte[] records) {}
