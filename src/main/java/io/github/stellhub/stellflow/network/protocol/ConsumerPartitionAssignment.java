package io.github.stellhub.stellflow.network.protocol;

/**
 * Consumer 分区分配。
 */
public record ConsumerPartitionAssignment(String topic, int partition) {}
