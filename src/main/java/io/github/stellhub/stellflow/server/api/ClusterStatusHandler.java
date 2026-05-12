package io.github.stellhub.stellflow.server.api;

import io.github.stellhub.stellflow.metadata.MetadataCache;
import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.ClusterStatusResponseBody;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.network.protocol.ResponseHeader;

/**
 * 集群状态与健康检查处理器。
 */
public class ClusterStatusHandler implements ApiHandler {

    private final ApiKey apiKey;
    private final MetadataCache metadataCache;

    public ClusterStatusHandler(ApiKey apiKey, MetadataCache metadataCache) {
        this.apiKey = apiKey;
        this.metadataCache = metadataCache;
    }

    @Override
    public ApiKey apiKey() {
        return apiKey;
    }

    @Override
    public ResponseContext handle(RequestContext requestContext) {
        int topicCount = metadataCache.topicNames().size();
        int partitionCount =
                metadataCache.topicNames().stream()
                        .mapToInt(topic -> metadataCache.topicPartitions(topic).size())
                        .sum();
        ClusterStatusResponseBody body =
                new ClusterStatusResponseBody(
                        ErrorCode.NONE,
                        "stellflow-dev-cluster",
                        metadataCache.brokers().size(),
                        topicCount,
                        partitionCount);
        return ResponseContext.builder()
                .requestContext(requestContext)
                .apiKey(apiKey)
                .apiVersion((short) 0)
                .responseHeader(new ResponseHeader(requestContext.getCorrelationId(), (short) 2, ErrorCode.NONE, 0))
                .responseBody(body)
                .build();
    }
}
