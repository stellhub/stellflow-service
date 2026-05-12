package io.github.stellhub.stellflow.network.protocol;

import java.util.List;

/**
 * OffsetFetch 请求体。
 */
public record OffsetFetchRequestBody(String groupId, List<OffsetFetchTopic> topics)
        implements RequestBody {}
