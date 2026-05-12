package io.github.stellhub.stellflow.network.protocol;

import java.util.List;

/**
 * OffsetCommit topic 响应。
 */
public record OffsetCommitTopicResponse(String topic, List<OffsetCommitPartitionResponse> partitions) {}
