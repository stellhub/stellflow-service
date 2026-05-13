package io.github.stellhub.stellflow.server.api;

import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.network.protocol.InitProducerIdRequestBody;
import io.github.stellhub.stellflow.network.protocol.InitProducerIdResponseBody;
import io.github.stellhub.stellflow.network.protocol.ResponseHeader;
import io.github.stellhub.stellflow.producer.ProducerState;
import io.github.stellhub.stellflow.producer.ProducerStateManager;

/**
 * InitProducerId 请求处理器。
 */
public class InitProducerIdHandler implements ApiHandler {

    private final ProducerStateManager producerStateManager;

    public InitProducerIdHandler(ProducerStateManager producerStateManager) {
        this.producerStateManager = producerStateManager;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.INIT_PRODUCER_ID;
    }

    @Override
    public ResponseContext handle(RequestContext requestContext) {
        InitProducerIdRequestBody body = (InitProducerIdRequestBody) requestContext.getRequestBody();
        ProducerState state =
                producerStateManager.initProducer(
                        body.transactionalId() == null || body.transactionalId().isBlank()
                                ? requestContext.getClientId()
                                : body.transactionalId());
        return ResponseContext.builder()
                .requestContext(requestContext)
                .apiKey(ApiKey.INIT_PRODUCER_ID)
                .apiVersion((short) 0)
                .responseHeader(
                        new ResponseHeader(
                                requestContext.getCorrelationId(), (short) 2, ErrorCode.NONE, 0))
                .responseBody(
                        new InitProducerIdResponseBody(
                                ErrorCode.NONE, state.producerId(), state.producerEpoch()))
                .build();
    }
}
