package io.github.stellhub.stellflow.network.protocol;

import java.util.List;

/**
 * Topic 管理请求体。
 */
public record TopicAdminRequestBody(
        String topic,
        int partitionCount,
        int partition,
        int leaderId,
        int leaderEpoch,
        List<Integer> replicaNodes,
        List<Integer> isrNodes)
        implements RequestBody {

    public TopicAdminRequestBody {
        replicaNodes = replicaNodes == null ? List.of() : List.copyOf(replicaNodes);
        isrNodes = isrNodes == null ? List.of() : List.copyOf(isrNodes);
    }
}
