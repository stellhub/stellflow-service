package io.github.stellhub.stellflow.network.protocol;

import java.util.List;

/**
 * Produce 响应体。
 */
public record ProduceResponseBody(List<ProduceTopicResponse> responses) implements ResponseBody {}
