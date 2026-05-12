package io.github.stellhub.stellflow.network.protocol;

/**
 * FindCoordinator 请求体。
 */
public record FindCoordinatorRequestBody(String key, byte keyType) implements RequestBody {}
