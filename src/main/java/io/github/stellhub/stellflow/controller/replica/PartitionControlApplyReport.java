package io.github.stellhub.stellflow.controller.replica;

/**
 * broker 应用控制命令后的结果回报。
 */
public record PartitionControlApplyReport(
        String topic,
        int partition,
        int leaderEpoch,
        boolean success,
        String message,
        long appliedAtMs) {}
