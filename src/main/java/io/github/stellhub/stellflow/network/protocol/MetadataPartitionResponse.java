package io.github.stellhub.stellflow.network.protocol;

import java.util.List;

/**
 * Metadata 分区响应。
 */
public record MetadataPartitionResponse(
        ErrorCode errorCode,
        int partition,
        int leaderId,
        int leaderEpoch,
        List<Integer> replicaNodes,
        List<Integer> isrNodes,
        List<Integer> offlineReplicaNodes) {}
