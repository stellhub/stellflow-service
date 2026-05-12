package io.github.stellhub.stellflow.network.protocol;

import java.util.List;

/**
 * OffsetFetch topic 项。
 */
public record OffsetFetchTopic(String topic, List<OffsetFetchPartition> partitions) {}
