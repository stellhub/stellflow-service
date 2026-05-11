package io.github.stellhub.stellflow.network.protocol;

import java.util.List;

/**
 * Fetch topic 请求。
 */
public record FetchTopicRequest(String topic, List<FetchPartitionRequest> partitions) {}
