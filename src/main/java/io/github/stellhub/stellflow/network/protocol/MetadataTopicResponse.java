package io.github.stellhub.stellflow.network.protocol;

import java.util.List;

/**
 * Metadata Topic 响应。
 */
public record MetadataTopicResponse(
        ErrorCode errorCode,
        String topic,
        boolean internal,
        List<MetadataPartitionResponse> partitions,
        int topicAuthorizedOperations) {}
