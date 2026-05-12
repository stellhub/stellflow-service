package io.github.stellhub.stellflow.network.protocol;

/**
 * SyncGroup 响应体。
 */
public record SyncGroupResponseBody(ErrorCode errorCode) implements ResponseBody {}
