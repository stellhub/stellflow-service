package io.github.stellhub.stellflow.network.protocol;

/**
 * 未实现 API 的原始请求体。
 */
public record OpaqueRequestBody(ApiKey apiKey, short apiVersion, byte[] payload) implements RequestBody {}
