package io.github.stellhub.stellflow.network.protocol;

/**
 * 统一请求消息。
 */
public record RequestMessage(RequestHeader header, RequestBody body) {}
