package io.github.stellhub.stellflow.network.protocol;

import java.util.List;

/**
 * Metadata 请求体。
 */
public record MetadataRequestBody(
        List<MetadataTopicRequest> topics,
        boolean includeClusterAuthorizedOperations,
        boolean includeTopicAuthorizedOperations,
        boolean allowAutoTopicCreation)
        implements RequestBody {}
