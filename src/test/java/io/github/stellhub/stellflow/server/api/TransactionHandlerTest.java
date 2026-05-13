package io.github.stellhub.stellflow.server.api;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.network.protocol.InitProducerIdRequestBody;
import io.github.stellhub.stellflow.network.protocol.InitProducerIdResponseBody;
import io.github.stellhub.stellflow.network.protocol.TransactionRequestBody;
import io.github.stellhub.stellflow.network.protocol.TransactionResponseBody;
import io.github.stellhub.stellflow.producer.ProducerStateManager;
import org.junit.jupiter.api.Test;

/**
 * TransactionHandler 测试。
 */
class TransactionHandlerTest {

    /**
     * 验证事务 begin / commit 协议闭环。
     */
    @Test
    void shouldBeginAndCommitTransaction() {
        ProducerStateManager producerStateManager = new ProducerStateManager();
        InitProducerIdHandler initHandler = new InitProducerIdHandler(producerStateManager);
        TransactionHandler beginHandler =
                new TransactionHandler(ApiKey.BEGIN_TRANSACTION, producerStateManager);
        TransactionHandler endHandler =
                new TransactionHandler(ApiKey.END_TRANSACTION, producerStateManager);

        ResponseContext initResponse =
                initHandler.handle(context(ApiKey.INIT_PRODUCER_ID, new InitProducerIdRequestBody("tx-a")));
        InitProducerIdResponseBody initBody =
                (InitProducerIdResponseBody) initResponse.getResponseBody();
        ResponseContext beginResponse =
                beginHandler.handle(
                        context(
                                ApiKey.BEGIN_TRANSACTION,
                                new TransactionRequestBody(
                                        "tx-a", initBody.producerId(), initBody.producerEpoch(), false)));
        ResponseContext commitResponse =
                endHandler.handle(
                        context(
                                ApiKey.END_TRANSACTION,
                                new TransactionRequestBody(
                                        "tx-a", initBody.producerId(), initBody.producerEpoch(), true)));

        assertEquals(ErrorCode.NONE, initBody.errorCode());
        assertEquals(ErrorCode.NONE, ((TransactionResponseBody) beginResponse.getResponseBody()).errorCode());
        assertEquals("ONGOING", ((TransactionResponseBody) beginResponse.getResponseBody()).transactionState());
        assertEquals(ErrorCode.NONE, ((TransactionResponseBody) commitResponse.getResponseBody()).errorCode());
        assertEquals("COMMITTED", ((TransactionResponseBody) commitResponse.getResponseBody()).transactionState());
    }

    private static RequestContext context(ApiKey apiKey, io.github.stellhub.stellflow.network.protocol.RequestBody body) {
        return RequestContext.builder()
                .clientId("client-a")
                .apiKey(apiKey)
                .apiVersion((short) 0)
                .correlationId(1)
                .requestBody(body)
                .build();
    }
}
