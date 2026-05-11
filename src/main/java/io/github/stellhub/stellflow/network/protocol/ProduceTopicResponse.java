package io.github.stellhub.stellflow.network.protocol;

import java.util.List;

/**
 * Produce topic 响应。
 */
public record ProduceTopicResponse(String topic, List<ProducePartitionResponse> partitions) {}
