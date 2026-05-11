package io.github.stellhub.stellflow.network.protocol;

import java.util.List;

/**
 * Produce 请求体。
 */
public record ProduceRequestBody(
        String transactionalId, short acks, int timeoutMs, List<ProduceTopicData> topicData)
        implements RequestBody {}
