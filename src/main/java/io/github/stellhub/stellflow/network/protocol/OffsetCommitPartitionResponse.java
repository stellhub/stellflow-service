package io.github.stellhub.stellflow.network.protocol;

/**
 * OffsetCommit 分区响应。
 */
public record OffsetCommitPartitionResponse(int partition, ErrorCode errorCode) {}
