package io.github.stellhub.stellflow.server.api;

import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.EmptyResponseBody;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.network.protocol.MetadataBroker;
import io.github.stellhub.stellflow.network.protocol.MetadataPartitionResponse;
import io.github.stellhub.stellflow.network.protocol.MetadataRequestBody;
import io.github.stellhub.stellflow.network.protocol.MetadataResponseBody;
import io.github.stellhub.stellflow.network.protocol.MetadataTopicResponse;
import io.github.stellhub.stellflow.network.protocol.MetadataTopicRequest;
import io.github.stellhub.stellflow.network.protocol.OpaqueRequestBody;
import io.github.stellhub.stellflow.network.protocol.ResponseHeader;
import io.github.stellhub.stellflow.storage.log.LogManager;
import java.util.ArrayList;
import java.util.List;

/**
 * Metadata 请求处理器。
 */
public class MetadataHandler implements ApiHandler {

    private final LogManager logManager;
    private final String brokerHost;
    private final int brokerPort;

    public MetadataHandler(LogManager logManager, String brokerHost, int brokerPort) {
        this.logManager = logManager;
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
    }

    /**
     * 返回当前处理器负责的 API。
     */
    @Override
    public ApiKey apiKey() {
        return ApiKey.METADATA;
    }

    /**
     * 生成 Metadata 响应骨架。
     */
    @Override
    public ResponseContext handle(RequestContext requestContext) {
        if (requestContext.getApiVersion() != 0) {
            return ResponseContext.builder()
                    .requestContext(requestContext)
                    .apiKey(ApiKey.METADATA)
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

        if (!(requestContext.getRequestBody() instanceof MetadataRequestBody metadataRequestBody)) {
            if (requestContext.getRequestBody() instanceof OpaqueRequestBody) {
                return ResponseContext.builder()
                        .requestContext(requestContext)
                        .apiKey(ApiKey.METADATA)
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
            throw new IllegalStateException("Unexpected request body type for metadata request");
        }

        List<MetadataTopicResponse> topics = new ArrayList<>();
        for (MetadataTopicRequest topicRequest : metadataRequestBody.topics()) {
            if (!logManager.containsTopic(topicRequest.topic())) {
                topics.add(
                        new MetadataTopicResponse(
                                ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                                topicRequest.topic(),
                                false,
                                List.of(),
                                0));
                continue;
            }
            topics.add(buildTopicMetadata(topicRequest.topic(), false));
        }

        MetadataResponseBody responseBody =
                new MetadataResponseBody(
                        "stellflow-dev-cluster",
                        0,
                        List.of(new MetadataBroker(0, brokerHost, brokerPort, null)),
                        topics,
                        0);

        if (metadataRequestBody.topics().isEmpty()) {
            responseBody =
                    new MetadataResponseBody(
                            "stellflow-dev-cluster",
                            0,
                            List.of(new MetadataBroker(0, brokerHost, brokerPort, null)),
                            buildAllTopicMetadata(),
                            0);
        }

        return ResponseContext.builder()
                .requestContext(requestContext)
                .apiKey(ApiKey.METADATA)
                .apiVersion((short) 0)
                .responseHeader(
                        new ResponseHeader(
                                requestContext.getCorrelationId(), (short) 2, ErrorCode.NONE, 0))
                .responseBody(responseBody)
                .build();
    }

    /**
     * 构造全部 topic 的元数据视图。
     */
    private List<MetadataTopicResponse> buildAllTopicMetadata() {
        List<MetadataTopicResponse> topicResponses = new ArrayList<>();
        for (String topic : logManager.topicNames()) {
            topicResponses.add(buildTopicMetadata(topic, false));
        }
        if (topicResponses.isEmpty()) {
            topicResponses.add(buildPlaceholderTopic());
        }
        return topicResponses;
    }

    /**
     * 构造单个 topic 的元数据视图。
     */
    private MetadataTopicResponse buildTopicMetadata(String topic, boolean internal) {
        List<MetadataPartitionResponse> partitions = new ArrayList<>();
        for (Integer partition : logManager.partitions(topic)) {
            partitions.add(
                    new MetadataPartitionResponse(
                            ErrorCode.NONE,
                            partition,
                            logManager.leaderId(topic, partition),
                            logManager.leaderEpoch(topic, partition),
                            logManager.replicaNodes(topic, partition),
                            logManager.isrNodes(topic, partition),
                            List.of()));
        }
        return new MetadataTopicResponse(ErrorCode.NONE, topic, internal, partitions, 0);
    }

    /**
     * 构造空存储时的占位 topic。
     */
    private MetadataTopicResponse buildPlaceholderTopic() {
        return new MetadataTopicResponse(
                ErrorCode.NONE,
                "__empty__",
                true,
                List.of(
                        new MetadataPartitionResponse(
                                ErrorCode.NONE,
                                0,
                                0,
                                0,
                                List.of(0),
                                List.of(0),
                                List.of())),
                0);
    }
}
