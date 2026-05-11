package io.github.stellhub.stellflow.network.protocol;

import java.util.List;

/**
 * Fetch 请求体。
 */
public record FetchRequestBody(
        int replicaId,
        int maxWaitMs,
        int minBytes,
        int maxBytes,
        byte isolationLevel,
        int sessionId,
        List<FetchTopicRequest> topicPartitions)
        implements RequestBody {}
