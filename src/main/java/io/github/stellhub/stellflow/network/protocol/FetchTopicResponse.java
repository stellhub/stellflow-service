package io.github.stellhub.stellflow.network.protocol;

import java.util.List;

/**
 * Fetch topic 响应。
 */
public record FetchTopicResponse(String topic, List<FetchPartitionResponse> partitions) {}
