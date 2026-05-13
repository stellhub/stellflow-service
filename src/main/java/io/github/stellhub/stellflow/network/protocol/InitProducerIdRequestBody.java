package io.github.stellhub.stellflow.network.protocol;

/**
 * InitProducerId 请求体。
 */
public record InitProducerIdRequestBody(String transactionalId) implements RequestBody {}
