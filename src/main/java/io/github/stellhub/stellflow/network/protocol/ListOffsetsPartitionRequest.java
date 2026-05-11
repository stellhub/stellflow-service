package io.github.stellhub.stellflow.network.protocol;

/**
 * ListOffsets 分区请求。
 */
public record ListOffsetsPartitionRequest(
        int partition, int currentLeaderEpoch, long timestamp, int maxNumOffsets) {}
