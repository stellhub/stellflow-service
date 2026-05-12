package io.github.stellhub.stellflow.network.protocol;

/**
 * Heartbeat 响应体。
 */
public record HeartbeatResponseBody(ErrorCode errorCode) implements ResponseBody {}
