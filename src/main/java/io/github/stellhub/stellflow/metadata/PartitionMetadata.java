package io.github.stellhub.stellflow.metadata;

import java.util.List;

/**
 * 分区元数据快照。
 */
public record PartitionMetadata(
        String topic,
        int partition,
        int leaderId,
        int leaderEpoch,
        List<Integer> replicaNodes,
        List<Integer> isrNodes,
        PartitionRole localRole) {

    public PartitionMetadata {
        replicaNodes = replicaNodes == null ? List.of() : List.copyOf(replicaNodes);
        isrNodes = isrNodes == null ? List.of() : List.copyOf(isrNodes);
        localRole = localRole == null ? PartitionRole.OFFLINE : localRole;
    }
}
