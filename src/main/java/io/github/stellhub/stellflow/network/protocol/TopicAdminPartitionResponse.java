package io.github.stellhub.stellflow.network.protocol;

/**
 * Topic 管理分区响应。
 */
public record TopicAdminPartitionResponse(int partition, ErrorCode errorCode, int leaderEpoch) {}
