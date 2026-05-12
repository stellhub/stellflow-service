package io.github.stellhub.stellflow.network.protocol;

import java.util.List;

/**
 * Topic 管理响应体。
 */
public record TopicAdminResponseBody(String topic, List<TopicAdminPartitionResponse> partitions)
        implements ResponseBody {}
