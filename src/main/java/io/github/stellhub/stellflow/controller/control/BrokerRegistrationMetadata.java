package io.github.stellhub.stellflow.controller.control;

/**
 * Controller 侧 broker 注册元数据。
 */
public record BrokerRegistrationMetadata(
        int brokerId,
        String advertisedEndpoint,
        String advertisedHost,
        int advertisedPort,
        long registeredAtMs) {}
