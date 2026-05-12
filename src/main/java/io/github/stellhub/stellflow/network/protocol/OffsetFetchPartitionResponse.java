package io.github.stellhub.stellflow.network.protocol;

/**
 * OffsetFetch 分区响应。
 */
public record OffsetFetchPartitionResponse(
        int partition, long offset, String metadata, ErrorCode errorCode) {}
