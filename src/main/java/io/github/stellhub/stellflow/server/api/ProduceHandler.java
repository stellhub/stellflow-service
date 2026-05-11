package io.github.stellhub.stellflow.server.api;

import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.EmptyResponseBody;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.network.protocol.OpaqueRequestBody;
import io.github.stellhub.stellflow.network.protocol.ProducePartitionData;
import io.github.stellhub.stellflow.network.protocol.ProducePartitionResponse;
import io.github.stellhub.stellflow.network.protocol.ProduceRequestBody;
import io.github.stellhub.stellflow.network.protocol.ProduceResponseBody;
import io.github.stellhub.stellflow.network.protocol.ProduceTopicData;
import io.github.stellhub.stellflow.network.protocol.ProduceTopicResponse;
import io.github.stellhub.stellflow.network.protocol.ResponseHeader;
import io.github.stellhub.stellflow.storage.log.LogAppendResult;
import io.github.stellhub.stellflow.storage.log.LogManager;
import java.util.ArrayList;
import java.util.List;

/**
 * Produce 请求处理器。
 */
public class ProduceHandler implements ApiHandler {

    private final LogManager logManager;

    public ProduceHandler(LogManager logManager) {
        this.logManager = logManager;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.PRODUCE;
    }

    @Override
    public ResponseContext handle(RequestContext requestContext) {
        if (requestContext.getApiVersion() != 0) {
            return ResponseContext.builder()
                    .requestContext(requestContext)
                    .apiKey(ApiKey.PRODUCE)
                    .apiVersion((short) 0)
                    .responseHeader(
                            new ResponseHeader(
                                    requestContext.getCorrelationId(),
                                    (short) 2,
                                    ErrorCode.UNSUPPORTED_VERSION,
                                    0))
                    .responseBody(EmptyResponseBody.INSTANCE)
                    .build();
        }

        if (!(requestContext.getRequestBody() instanceof ProduceRequestBody produceRequestBody)) {
            if (requestContext.getRequestBody() instanceof OpaqueRequestBody) {
                return ResponseContext.builder()
                        .requestContext(requestContext)
                        .apiKey(ApiKey.PRODUCE)
                        .apiVersion((short) 0)
                        .responseHeader(
                                new ResponseHeader(
                                        requestContext.getCorrelationId(),
                                        (short) 2,
                                        ErrorCode.INVALID_REQUEST,
                                        0))
                        .responseBody(EmptyResponseBody.INSTANCE)
                        .build();
            }
            throw new IllegalStateException("Unexpected request body type for produce request");
        }

        List<ProduceTopicResponse> topicResponses = new ArrayList<>();
        for (ProduceTopicData topicData : produceRequestBody.topicData()) {
            List<ProducePartitionResponse> partitionResponses = new ArrayList<>();
            for (ProducePartitionData partitionData : topicData.partitions()) {
                if (partitionData.records() == null || partitionData.records().length == 0) {
                    partitionResponses.add(
                            new ProducePartitionResponse(
                                    partitionData.partition(),
                                    ErrorCode.INVALID_RECORD,
                                    -1,
                                    0,
                                    -1,
                                    0));
                    continue;
                }
                LogAppendResult appendResult =
                        logManager.append(topicData.topic(), partitionData.partition(), partitionData.records());
                partitionResponses.add(
                        new ProducePartitionResponse(
                                partitionData.partition(),
                                ErrorCode.NONE,
                                appendResult.baseOffset(),
                                appendResult.leaderEpoch(),
                                -1,
                                logManager.logStartOffset(topicData.topic(), partitionData.partition())));
            }
            topicResponses.add(new ProduceTopicResponse(topicData.topic(), partitionResponses));
        }

        return ResponseContext.builder()
                .requestContext(requestContext)
                .apiKey(ApiKey.PRODUCE)
                .apiVersion((short) 0)
                .responseHeader(
                        new ResponseHeader(
                                requestContext.getCorrelationId(), (short) 2, ErrorCode.NONE, 0))
                .responseBody(new ProduceResponseBody(topicResponses))
                .build();
    }
}
