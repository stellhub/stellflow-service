package io.github.stellhub.stellflow.server.api;

import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.network.protocol.ResponseHeader;
import io.github.stellhub.stellflow.network.protocol.TopicAdminPartitionResponse;
import io.github.stellhub.stellflow.network.protocol.TopicAdminRequestBody;
import io.github.stellhub.stellflow.network.protocol.TopicAdminResponseBody;
import io.github.stellhub.stellflow.server.runtime.ReplicaManager;
import java.util.List;

/**
 * Topic 管理请求处理器。
 */
public class TopicAdminHandler implements ApiHandler {

    private final ApiKey apiKey;
    private final ReplicaManager replicaManager;

    public TopicAdminHandler(ApiKey apiKey, ReplicaManager replicaManager) {
        this.apiKey = apiKey;
        this.replicaManager = replicaManager;
    }

    @Override
    public ApiKey apiKey() {
        return apiKey;
    }

    @Override
    public ResponseContext handle(RequestContext requestContext) {
        TopicAdminRequestBody body = (TopicAdminRequestBody) requestContext.getRequestBody();
        List<TopicAdminPartitionResponse> partitions =
                switch (apiKey) {
                    case CREATE_TOPIC -> replicaManager.createTopic(body.topic(), body.partitionCount()).stream()
                            .map(result -> new TopicAdminPartitionResponse(
                                    result.partition(), result.errorCode(), result.leaderEpoch()))
                            .toList();
                    case DELETE_TOPIC -> replicaManager.deleteTopic(body.topic()).stream()
                            .map(result -> new TopicAdminPartitionResponse(
                                    result.partition(), result.errorCode(), result.leaderEpoch()))
                            .toList();
                    case ALTER_PARTITION -> List.of(toResponse(body));
                    default -> List.of(new TopicAdminPartitionResponse(-1, ErrorCode.INVALID_REQUEST, 0));
                };
        return ResponseContext.builder()
                .requestContext(requestContext)
                .apiKey(apiKey)
                .apiVersion((short) 0)
                .responseHeader(new ResponseHeader(requestContext.getCorrelationId(), (short) 2, ErrorCode.NONE, 0))
                .responseBody(new TopicAdminResponseBody(body.topic(), partitions))
                .build();
    }

    private TopicAdminPartitionResponse toResponse(TopicAdminRequestBody body) {
        var result =
                replicaManager.alterPartition(
                        body.topic(),
                        body.partition(),
                        body.leaderId(),
                        body.leaderEpoch(),
                        body.replicaNodes(),
                        body.isrNodes());
        return new TopicAdminPartitionResponse(
                result.partition(), result.errorCode(), result.leaderEpoch());
    }
}
