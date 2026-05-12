package io.github.stellhub.stellflow.network.protocol;

/**
 * OffsetCommit 分区项。
 */
public record OffsetCommitPartition(int partition, long offset, String metadata) {}
