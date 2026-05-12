package io.github.stellhub.stellflow.network.protocol;

import java.util.List;

/**
 * OffsetFetch topic 响应。
 */
public record OffsetFetchTopicResponse(String topic, List<OffsetFetchPartitionResponse> partitions) {}
