package io.github.stellhub.stellflow.network.protocol;

/**
 * Broker 管理请求体。
 */
public record BrokerAdminRequestBody(int brokerId) implements RequestBody {}
