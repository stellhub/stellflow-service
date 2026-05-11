package io.github.stellhub.stellflow.network.protocol;

/**
 * Metadata Broker 信息。
 */
public record MetadataBroker(int brokerId, String host, int port, String rack) {}
