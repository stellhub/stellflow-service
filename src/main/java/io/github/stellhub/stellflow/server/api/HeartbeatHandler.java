package io.github.stellhub.stellflow.server.api;

import io.github.stellhub.stellflow.coordinator.ConsumerGroupCoordinator;
import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.network.protocol.HeartbeatRequestBody;
import io.github.stellhub.stellflow.network.protocol.HeartbeatResponseBody;
import io.github.stellhub.stellflow.observability.metrics.StellflowMetrics;
import io.github.stellhub.stellflow.network.protocol.ResponseHeader;

/**
 * Heartbeat 请求处理器。
 */
public class HeartbeatHandler implements ApiHandler {

    private final ConsumerGroupCoordinator coordinator;
    private final StellflowMetrics metrics;

    public HeartbeatHandler(ConsumerGroupCoordinator coordinator) {
        this(coordinator, StellflowMetrics.global());
    }

    public HeartbeatHandler(ConsumerGroupCoordinator coordinator, StellflowMetrics metrics) {
        this.coordinator = coordinator;
        this.metrics = metrics;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.HEARTBEAT;
    }

    @Override
    public ResponseContext handle(RequestContext requestContext) {
        long startMs = System.currentTimeMillis();
        HeartbeatRequestBody body = (HeartbeatRequestBody) requestContext.getRequestBody();
        ErrorCode errorCode = coordinator.heartbeat(body.groupId(), body.generationId(), body.memberId());
        metrics.recordGroup(
                ApiKey.HEARTBEAT,
                body.groupId(),
                errorCode,
                System.currentTimeMillis() - startMs);
        return ResponseContext.builder()
                .requestContext(requestContext)
                .apiKey(ApiKey.HEARTBEAT)
                .apiVersion((short) 0)
                .responseHeader(new ResponseHeader(requestContext.getCorrelationId(), (short) 2, errorCode, 0))
                .responseBody(new HeartbeatResponseBody(errorCode))
                .build();
    }
}
