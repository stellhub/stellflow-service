package io.github.stellhub.stellflow.controller.replica;

/**
 * 单分区副本抓取分配。
 */
public record ReplicaFetchAssignment(
        String topic, int partition, String leaderHost, int leaderPort, int leaderBrokerId) {}
