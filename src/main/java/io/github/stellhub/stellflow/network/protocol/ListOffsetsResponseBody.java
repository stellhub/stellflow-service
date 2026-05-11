package io.github.stellhub.stellflow.network.protocol;

import java.util.List;

/**
 * ListOffsets 响应体。
 */
public record ListOffsetsResponseBody(List<ListOffsetsTopicResponse> topics)
        implements ResponseBody {}
