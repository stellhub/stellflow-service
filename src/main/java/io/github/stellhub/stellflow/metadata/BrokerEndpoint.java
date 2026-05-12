package io.github.stellhub.stellflow.metadata;

/**
 * Broker 对外访问端点。
 */
public record BrokerEndpoint(int brokerId, String host, int port, String rack) {}
