package io.github.stellhub.stellflow.server.api;

import io.github.stellhub.stellflow.coordinator.ConsumerGroupCoordinator;
import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.network.protocol.JoinGroupRequestBody;
import io.github.stellhub.stellflow.network.protocol.JoinGroupResponseBody;
import io.github.stellhub.stellflow.network.protocol.ResponseHeader;

/**
 * JoinGroup 请求处理器。
 */
public class JoinGroupHandler implements ApiHandler {

    private final ConsumerGroupCoordinator coordinator;

    public JoinGroupHandler(ConsumerGroupCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.JOIN_GROUP;
    }

    @Override
    public ResponseContext handle(RequestContext requestContext) {
        JoinGroupRequestBody body = (JoinGroupRequestBody) requestContext.getRequestBody();
        ConsumerGroupCoordinator.JoinResult result =
                coordinator.joinGroup(
                        body.groupId(),
                        body.memberId(),
                        requestContext.getClientId(),
                        "unknown",
                        body.sessionTimeoutMs());
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
