package io.github.stellhub.stellflow.network.protocol;

import java.util.List;

/**
 * OffsetCommit 请求体。
 */
public record OffsetCommitRequestBody(String groupId, List<OffsetCommitTopic> topics)
        implements RequestBody {}
