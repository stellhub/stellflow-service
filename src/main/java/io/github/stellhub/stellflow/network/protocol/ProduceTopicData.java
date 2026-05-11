package io.github.stellhub.stellflow.network.protocol;

import java.util.List;

/**
 * Produce topic 载荷。
 */
public record ProduceTopicData(String topic, List<ProducePartitionData> partitions) {}
