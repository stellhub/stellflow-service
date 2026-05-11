package io.github.stellhub.stellflow.network.protocol;

import java.util.List;

/**
 * ListOffsets topic 响应。
 */
public record ListOffsetsTopicResponse(
        String topic, List<ListOffsetsPartitionResponse> partitions) {}
