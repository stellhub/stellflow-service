package io.github.stellhub.stellflow.network.protocol;

import java.util.List;

/**
 * Fetch 响应体。
 */
public record FetchResponseBody(int sessionId, List<FetchTopicResponse> responses)
        implements ResponseBody {}
