package io.github.stellhub.stellflow.network.protocol;

import java.util.List;

/**
 * ListOffsets topic 请求。
 */
public record ListOffsetsTopicRequest(String topic, List<ListOffsetsPartitionRequest> partitions) {}
