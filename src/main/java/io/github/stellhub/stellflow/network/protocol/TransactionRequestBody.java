package io.github.stellhub.stellflow.network.protocol;

/**
 * 事务控制请求体。
 */
public record TransactionRequestBody(
        String transactionalId, long producerId, short producerEpoch, boolean commit)
        implements RequestBody {}
