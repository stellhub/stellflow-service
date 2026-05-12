package io.github.stellhub.stellflow.network.protocol;

/**
 * Broker 管理响应体。
 */
public record BrokerAdminResponseBody(ErrorCode errorCode, String message) implements ResponseBody {}
