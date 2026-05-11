package io.github.stellhub.stellflow.network.protocol;

import java.util.List;

/**
 * ListOffsets 请求体。
 */
public record ListOffsetsRequestBody(
        int replicaId, byte isolationLevel, List<ListOffsetsTopicRequest> topics)
        implements RequestBody {}
