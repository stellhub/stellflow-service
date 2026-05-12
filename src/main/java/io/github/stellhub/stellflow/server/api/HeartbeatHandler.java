package io.github.stellhub.stellflow.server.api;

import io.github.stellhub.stellflow.coordinator.ConsumerGroupCoordinator;
import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.network.protocol.HeartbeatRequestBody;
import io.github.stellhub.stellflow.network.protocol.HeartbeatResponseBody;
import io.github.stellhub.stellflow.network.protocol.ResponseHeader;

/**
 * Heartbeat 请求处理器。
 */
public class HeartbeatHandler implements ApiHandler {

    private final ConsumerGroupCoordinator coordinator;

    public HeartbeatHandler(ConsumerGroupCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.HEARTBEAT;
    }

    @Override
    public ResponseContext handle(RequestContext requestContext) {
        HeartbeatRequestBody body = (HeartbeatRequestBody) requestContext.getRequestBody();
        ErrorCode errorCode = coordinator.heartbeat(body.groupId(), body.generationId(), body.memberId());
        return ResponseContext.builder()
                .requestContext(requestContext)
                .apiKey(ApiKey.HEARTBEAT)
                .apiVersion((short) 0)
                .responseHeader(new ResponseHeader(requestContext.getCorrelationId(), (short) 2, errorCode, 0))
                .responseBody(new HeartbeatResponseBody(errorCode))
                .build();
    }
}
