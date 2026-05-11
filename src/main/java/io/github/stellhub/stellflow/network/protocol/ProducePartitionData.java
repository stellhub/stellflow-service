package io.github.stellhub.stellflow.network.protocol;

/**
 * Produce 分区载荷。
 */
public record ProducePartitionData(int partition, byte[] records) {}
