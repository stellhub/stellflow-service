package io.github.stellhub.stellflow.network.protocol;

import java.util.List;

/**
 * OffsetCommit 响应体。
 */
public record OffsetCommitResponseBody(List<OffsetCommitTopicResponse> topics) implements ResponseBody {}
