package io.github.stellhub.stellflow.network.protocol;

/**
 * 统一响应头。
 */
public record ResponseHeader(
        int correlationId,
        short headerVersion,
        ErrorCode errorCode,
        int throttleTimeMs) {}
