package io.github.stellhub.stellflow.controller.control;

import java.util.List;
import lombok.Builder;

/**
 * Controller 侧分区期望元数据。
 */
@Builder
public record ControllerPartitionMetadata(
        String topic,
        int partition,
        int leaderId,
        int leaderEpoch,
        List<Integer> replicaNodes,
        List<Integer> isrNodes,
        Integer truncateToLeaderEpoch,
        Long truncateToOffset) {}
