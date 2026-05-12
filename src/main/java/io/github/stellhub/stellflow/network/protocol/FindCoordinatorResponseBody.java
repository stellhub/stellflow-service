package io.github.stellhub.stellflow.network.protocol;

/**
 * FindCoordinator 响应体。
 */
public record FindCoordinatorResponseBody(ErrorCode errorCode, int nodeId, String host, int port)
        implements ResponseBody {}
