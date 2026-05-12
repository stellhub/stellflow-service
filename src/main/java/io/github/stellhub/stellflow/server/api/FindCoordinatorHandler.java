package io.github.stellhub.stellflow.server.api;

import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.network.protocol.FindCoordinatorResponseBody;
import io.github.stellhub.stellflow.network.protocol.ResponseHeader;

/**
 * FindCoordinator 请求处理器。
 */
public class FindCoordinatorHandler implements ApiHandler {

    private final int brokerId;
    private final String host;
    private final int port;

    public FindCoordinatorHandler(int brokerId, String host, int port) {
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.FIND_COORDINATOR;
    }

    @Override
    public ResponseContext handle(RequestContext requestContext) {
        return ResponseContext.builder()
                .requestContext(requestContext)
                .apiKey(ApiKey.FIND_COORDINATOR)
                .apiVersion((short) 0)
                .responseHeader(
                        new ResponseHeader(
                                requestContext.getCorrelationId(), (short) 2, ErrorCode.NONE, 0))
                .responseBody(new FindCoordinatorResponseBody(ErrorCode.NONE, brokerId, host, port))
                .build();
    }
}
