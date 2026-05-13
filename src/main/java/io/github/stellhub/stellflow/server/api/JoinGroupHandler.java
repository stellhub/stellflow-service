package io.github.stellhub.stellflow.server.api;

import io.github.stellhub.stellflow.coordinator.ConsumerGroupCoordinator;
import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.network.protocol.JoinGroupRequestBody;
import io.github.stellhub.stellflow.network.protocol.JoinGroupResponseBody;
import io.github.stellhub.stellflow.observability.metrics.StellflowMetrics;
import io.github.stellhub.stellflow.network.protocol.ResponseHeader;

/**
 * JoinGroup 请求处理器。
 */
public class JoinGroupHandler implements ApiHandler {

    private final ConsumerGroupCoordinator coordinator;
    private final StellflowMetrics metrics;

    public JoinGroupHandler(ConsumerGroupCoordinator coordinator) {
        this(coordinator, StellflowMetrics.global());
    }

    public JoinGroupHandler(ConsumerGroupCoordinator coordinator, StellflowMetrics metrics) {
        this.coordinator = coordinator;
        this.metrics = metrics;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.JOIN_GROUP;
    }

    @Override
    public ResponseContext handle(RequestContext requestContext) {
        long startMs = System.currentTimeMillis();
        JoinGroupRequestBody body = (JoinGroupRequestBody) requestContext.getRequestBody();
        ConsumerGroupCoordinator.JoinResult result =
                coordinator.joinGroup(
                        body.groupId(),
                        body.memberId(),
                        requestContext.getClientId(),
                        "unknown",
                        body.sessionTimeoutMs());
        metrics.recordGroup(
                ApiKey.JOIN_GROUP,
                body.groupId(),
                result.errorCode(),
                System.currentTimeMillis() - startMs);
        return ResponseContext.builder()
                .requestContext(requestContext)
                .apiKey(ApiKey.JOIN_GROUP)
                .apiVersion((short) 0)
                .responseHeader(new ResponseHeader(requestContext.getCorrelationId(), (short) 2, ErrorCode.NONE, 0))
                .responseBody(
                        new JoinGroupResponseBody(
                                result.errorCode(),
                                result.generationId(),
                                result.memberId(),
                                result.leaderId()))
                .build();
    }
}
