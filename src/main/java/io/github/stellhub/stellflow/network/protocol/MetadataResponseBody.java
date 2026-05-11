package io.github.stellhub.stellflow.network.protocol;

import java.util.List;

/**
 * Metadata 响应体。
 */
public record MetadataResponseBody(
        String clusterId,
        int controllerId,
        List<MetadataBroker> brokers,
        List<MetadataTopicResponse> topics,
        int clusterAuthorizedOperations)
        implements ResponseBody {}
