package io.github.stellhub.stellflow.network.protocol;

/**
 * 事务控制响应体。
 */
public record TransactionResponseBody(
        ErrorCode errorCode, long producerId, short producerEpoch, String transactionState)
        implements ResponseBody {}
