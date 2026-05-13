package io.github.stellhub.stellflow.server.api;

import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.network.protocol.ResponseHeader;
import io.github.stellhub.stellflow.network.protocol.TransactionRequestBody;
import io.github.stellhub.stellflow.network.protocol.TransactionResponseBody;
import io.github.stellhub.stellflow.producer.ProducerStateManager;

/**
 * 事务控制请求处理器。
 */
public class TransactionHandler implements ApiHandler {

    private final ApiKey apiKey;
    private final ProducerStateManager producerStateManager;

    public TransactionHandler(ApiKey apiKey, ProducerStateManager producerStateManager) {
        this.apiKey = apiKey;
        this.producerStateManager = producerStateManager;
    }

    @Override
    public ApiKey apiKey() {
        return apiKey;
    }

    @Override
    public ResponseContext handle(RequestContext requestContext) {
        TransactionRequestBody body = (TransactionRequestBody) requestContext.getRequestBody();
        ErrorCode errorCode;
        String state;
        if (apiKey == ApiKey.BEGIN_TRANSACTION) {
            errorCode =
                    producerStateManager.beginTransaction(
                            body.transactionalId(), body.producerId(), body.producerEpoch());
            state = errorCode == ErrorCode.NONE ? "ONGOING" : "FAILED";
        } else if (body.commit()) {
            errorCode =
                    producerStateManager.commitTransaction(
                            body.producerId(), body.producerEpoch());
            state = errorCode == ErrorCode.NONE ? "COMMITTED" : "FAILED";
        } else {
            errorCode =
                    producerStateManager.abortTransaction(body.producerId(), body.producerEpoch());
            state = errorCode == ErrorCode.NONE ? "ABORTED" : "FAILED";
        }
        return ResponseContext.builder()
                .requestContext(requestContext)
                .apiKey(apiKey)
                .apiVersion((short) 0)
                .responseHeader(
                        new ResponseHeader(
                                requestContext.getCorrelationId(), (short) 2, errorCode, 0))
                .responseBody(
                        new TransactionResponseBody(
                                errorCode, body.producerId(), body.producerEpoch(), state))
                .build();
    }
}
