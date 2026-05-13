package io.github.stellhub.stellflow.coordinator;

import io.github.stellhub.stellflow.storage.log.TopicPartition;
import java.util.List;

/**
 * 消费组成员分区分配。
 */
public record PartitionAssignment(String memberId, List<TopicPartition> partitions) {}
