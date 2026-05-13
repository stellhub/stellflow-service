package io.github.stellhub.stellflow.network.protocol;

/**
 * InitProducerId 响应体。
 */
public record InitProducerIdResponseBody(
        ErrorCode errorCode, long producerId, short producerEpoch)
        implements ResponseBody {}
