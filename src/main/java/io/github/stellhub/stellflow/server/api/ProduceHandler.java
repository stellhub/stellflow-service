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
import io.github.stellhub.stellflow.observability.logging.MessageFlowDebugLogConfig;
import io.github.stellhub.stellflow.observability.metrics.StellflowMetrics;
import io.github.stellhub.stellflow.producer.ProducerAppendDecision;
import io.github.stellhub.stellflow.producer.ProducerStateManager;
import io.github.stellhub.stellflow.server.runtime.PartitionAppendResult;
import io.github.stellhub.stellflow.server.runtime.ReplicaManager;
import io.github.stellhub.stellflow.storage.log.LogAppendResult;
import io.github.stellhub.stellflow.storage.log.LogManager;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * Produce 请求处理器。
 */
@Slf4j
public class ProduceHandler implements ApiHandler {

    private final LogManager logManager;
    private final ReplicaManager replicaManager;
    private final ProducerStateManager producerStateManager;
    private final StellflowMetrics metrics;
    private final MessageFlowDebugLogConfig debugLogConfig;

    public ProduceHandler(LogManager logManager) {
        this.logManager = logManager;
        this.replicaManager = null;
        this.producerStateManager = new ProducerStateManager();
        this.metrics = StellflowMetrics.global();
        this.debugLogConfig = MessageFlowDebugLogConfig.load();
    }

    public ProduceHandler(ReplicaManager replicaManager) {
        this(replicaManager, new ProducerStateManager());
    }

    public ProduceHandler(ReplicaManager replicaManager, ProducerStateManager producerStateManager) {
        this(replicaManager, producerStateManager, StellflowMetrics.global());
    }

    public ProduceHandler(
            ReplicaManager replicaManager,
            ProducerStateManager producerStateManager,
            StellflowMetrics metrics) {
        this(replicaManager, producerStateManager, metrics, MessageFlowDebugLogConfig.load());
    }

    public ProduceHandler(
            ReplicaManager replicaManager,
            ProducerStateManager producerStateManager,
            StellflowMetrics metrics,
            MessageFlowDebugLogConfig debugLogConfig) {
        this.logManager = null;
        this.replicaManager = replicaManager;
        this.producerStateManager = producerStateManager;
        this.metrics = metrics;
        this.debugLogConfig = debugLogConfig;
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
        producerStateManager.getOrCreate(producerKey(requestContext, produceRequestBody));
        for (ProduceTopicData topicData : produceRequestBody.topicData()) {
            List<ProducePartitionResponse> partitionResponses = new ArrayList<>();
            for (ProducePartitionData partitionData : topicData.partitions()) {
                long partitionStartMs = System.currentTimeMillis();
                if (partitionData.records() == null || partitionData.records().length == 0) {
                    recordProduce(topicData.topic(), partitionData.partition(), ErrorCode.INVALID_RECORD, 0, partitionStartMs);
                    logProduceDebug(
                            requestContext,
                            produceRequestBody,
                            topicData.topic(),
                            partitionData.partition(),
                            0,
                            ErrorCode.INVALID_RECORD,
                            -1,
                            -1);
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
                if (produceRequestBody.acks() < -1 || produceRequestBody.acks() > 1) {
                    recordProduce(topicData.topic(), partitionData.partition(), ErrorCode.INVALID_REQUEST, 0, partitionStartMs);
                    logProduceDebug(
                            requestContext,
                            produceRequestBody,
                            topicData.topic(),
                            partitionData.partition(),
                            partitionData.records().length,
                            ErrorCode.INVALID_REQUEST,
                            -1,
                            -1);
                    partitionResponses.add(
                            new ProducePartitionResponse(
                                    partitionData.partition(),
                                    ErrorCode.INVALID_REQUEST,
                                    -1,
                                    0,
                                    -1,
                                    0));
                    continue;
                }
                ProducerAppendDecision decision =
                        producerStateManager.validateAppend(
                                producerKey(requestContext, produceRequestBody),
                                topicData.topic(),
                                partitionData.partition(),
                                partitionData.records());
                if (decision.errorCode() != ErrorCode.NONE) {
                    recordProduce(
                            topicData.topic(),
                            partitionData.partition(),
                            decision.errorCode(),
                            partitionData.records().length,
                            partitionStartMs);
                    logProduceDebug(
                            requestContext,
                            produceRequestBody,
                            topicData.topic(),
                            partitionData.partition(),
                            partitionData.records().length,
                            decision.errorCode(),
                            -1,
                            -1);
                    partitionResponses.add(
                            new ProducePartitionResponse(
                                    partitionData.partition(),
                                    decision.errorCode(),
                                    -1,
                                    0,
                                    -1,
                                    0));
                    continue;
                }
                if (!decision.appendRequired()) {
                    recordProduce(
                            topicData.topic(),
                            partitionData.partition(),
                            ErrorCode.NONE,
                            0,
                            partitionStartMs);
                    logProduceDebug(
                            requestContext,
                            produceRequestBody,
                            topicData.topic(),
                            partitionData.partition(),
                            partitionData.records().length,
                            ErrorCode.NONE,
                            decision.duplicateBaseOffset(),
                            -1);
                    partitionResponses.add(
                            new ProducePartitionResponse(
                                    partitionData.partition(),
                                    ErrorCode.NONE,
                                    decision.duplicateBaseOffset(),
                                    0,
                                    -1,
                                    0));
                    continue;
                }
                PartitionAppendResult appendResult = append(produceRequestBody, topicData, partitionData);
                recordProduce(
                        topicData.topic(),
                        partitionData.partition(),
                        appendResult.errorCode(),
                        partitionData.records().length,
                        partitionStartMs);
                if (appendResult.errorCode() == ErrorCode.NONE) {
                    producerStateManager.recordAppendSuccess(
                            decision.batchInfo(),
                            topicData.topic(),
                            partitionData.partition(),
                            appendResult.baseOffset());
                }
                logProduceDebug(
                        requestContext,
                        produceRequestBody,
                        topicData.topic(),
                        partitionData.partition(),
                        partitionData.records().length,
                        appendResult.errorCode(),
                        appendResult.baseOffset(),
                        appendResult.logEndOffset());
                partitionResponses.add(
                        new ProducePartitionResponse(
                                partitionData.partition(),
                                appendResult.errorCode(),
                                appendResult.baseOffset(),
                                appendResult.leaderEpoch(),
                                -1,
                                appendResult.logStartOffset()));
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

    private PartitionAppendResult append(
            ProduceRequestBody produceRequestBody,
            ProduceTopicData topicData,
            ProducePartitionData partitionData) {
        if (replicaManager != null) {
            return replicaManager.append(
                    topicData.topic(),
                    partitionData.partition(),
                    partitionData.records(),
                    produceRequestBody.acks(),
                    produceRequestBody.timeoutMs());
        }
        LogAppendResult appendResult =
                logManager.append(topicData.topic(), partitionData.partition(), partitionData.records());
        return new PartitionAppendResult(
                ErrorCode.NONE,
                appendResult.baseOffset(),
                0,
                logManager.logStartOffset(topicData.topic(), partitionData.partition()),
                appendResult.leaderEpoch(),
                logManager.highWatermark(topicData.topic(), partitionData.partition()),
                appendResult.logEndOffset());
    }

    private String producerKey(RequestContext requestContext, ProduceRequestBody body) {
        if (body.transactionalId() != null && !body.transactionalId().isBlank()) {
            return body.transactionalId();
        }
        return requestContext.getClientId();
    }

    private void recordProduce(
            String topic, int partition, ErrorCode errorCode, long bytes, long startMs) {
        metrics.recordProduce(topic, partition, errorCode, bytes, System.currentTimeMillis() - startMs);
    }

    private void logProduceDebug(
            RequestContext requestContext,
            ProduceRequestBody body,
            String topic,
            int partition,
            int recordBytes,
            ErrorCode errorCode,
            long baseOffset,
            long logEndOffset) {
        if (!debugLogConfig.isEnabled()) {
            return;
        }
        log.info(
                "Produce message debug connectionId={} clientId={} correlationId={} traceId={} topic={} partition={} recordBytes={} acks={} timeoutMs={} errorCode={} baseOffset={} logEndOffset={}",
                requestContext.getConnectionId(),
                requestContext.getClientId(),
                requestContext.getCorrelationId(),
                requestContext.getTraceId(),
                topic,
                partition,
                recordBytes,
                body.acks(),
                body.timeoutMs(),
                errorCode,
                baseOffset,
                logEndOffset);
    }
}
