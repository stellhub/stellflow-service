package io.github.stellhub.stellflow.network.protocol;

/**
 * Fetch 分区请求。
 */
public record FetchPartitionRequest(
        int partition,
        int currentLeaderEpoch,
        long fetchOffset,
        long logStartOffset,
        int partitionMaxBytes) {}
