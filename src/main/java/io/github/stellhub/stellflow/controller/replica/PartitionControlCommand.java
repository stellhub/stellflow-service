package io.github.stellhub.stellflow.controller.replica;

import java.util.List;

/**
 * Controller 下发到副本层的分区控制命令。
 */
public record PartitionControlCommand(
        String topic,
        int partition,
        int leaderId,
        int leaderEpoch,
        List<Integer> replicaNodes,
        List<Integer> isrNodes,
        Integer truncateToLeaderEpoch,
        Long truncateToOffset) {}
