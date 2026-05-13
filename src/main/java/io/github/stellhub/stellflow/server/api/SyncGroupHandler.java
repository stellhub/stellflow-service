package io.github.stellhub.stellflow.server.api;

import io.github.stellhub.stellflow.coordinator.ConsumerGroupCoordinator;
import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.ConsumerPartitionAssignment;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.network.protocol.ResponseHeader;
import io.github.stellhub.stellflow.network.protocol.SyncGroupRequestBody;
import io.github.stellhub.stellflow.network.protocol.SyncGroupResponseBody;

/**
 * SyncGroup 请求处理器。
 */
public class SyncGroupHandler implements ApiHandler {

    private final ConsumerGroupCoordinator coordinator;

    public SyncGroupHandler(ConsumerGroupCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.SYNC_GROUP;
    }

    @Override
    public ResponseContext handle(RequestContext requestContext) {
        SyncGroupRequestBody body = (SyncGroupRequestBody) requestContext.getRequestBody();
        ErrorCode errorCode = coordinator.syncGroup(body.groupId(), body.generationId(), body.memberId());
        var assignments =
                errorCode == ErrorCode.NONE
                        ? coordinator.assignment(body.groupId(), body.memberId()).stream()
                                .map(value -> new ConsumerPartitionAssignment(value.topic(), value.partition()))
                                .toList()
                        : java.util.List.<ConsumerPartitionAssignment>of();
        return ResponseContext.builder()
                .requestContext(requestContext)
                .apiKey(ApiKey.SYNC_GROUP)
                .apiVersion((short) 0)
                .responseHeader(new ResponseHeader(requestContext.getCorrelationId(), (short) 2, errorCode, 0))
                .responseBody(new SyncGroupResponseBody(errorCode, assignments))
                .build();
    }
}
