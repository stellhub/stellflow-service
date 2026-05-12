package io.github.stellhub.stellflow.network.protocol;

import java.util.List;

/**
 * OffsetFetch 响应体。
 */
public record OffsetFetchResponseBody(List<OffsetFetchTopicResponse> topics) implements ResponseBody {}
