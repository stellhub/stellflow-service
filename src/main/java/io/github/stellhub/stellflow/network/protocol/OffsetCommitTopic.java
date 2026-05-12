package io.github.stellhub.stellflow.network.protocol;

import java.util.List;

/**
 * OffsetCommit topic 项。
 */
public record OffsetCommitTopic(String topic, List<OffsetCommitPartition> partitions) {}
