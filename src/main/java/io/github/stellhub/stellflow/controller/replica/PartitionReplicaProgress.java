package io.github.stellhub.stellflow.controller.replica;

import java.util.List;

/**
 * 单分区副本复制进度快照。
 */
public record PartitionReplicaProgress(
        String topic,
        int partition,
        int leaderId,
        int leaderEpoch,
        long logEndOffset,
        long highWatermark,
        List<Integer> replicaNodes,
        List<Integer> isrNodes) {}
