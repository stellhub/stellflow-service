package io.github.stellhub.stellflow.server.api;

import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.EmptyResponseBody;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.network.protocol.ListOffsetsPartitionRequest;
import io.github.stellhub.stellflow.network.protocol.ListOffsetsPartitionResponse;
import io.github.stellhub.stellflow.network.protocol.ListOffsetsRequestBody;
import io.github.stellhub.stellflow.network.protocol.ListOffsetsResponseBody;
import io.github.stellhub.stellflow.network.protocol.ListOffsetsTopicRequest;
import io.github.stellhub.stellflow.network.protocol.ListOffsetsTopicResponse;
import io.github.stellhub.stellflow.network.protocol.OpaqueRequestBody;
import io.github.stellhub.stellflow.network.protocol.ResponseHeader;
import io.github.stellhub.stellflow.storage.log.LogManager;
import io.github.stellhub.stellflow.storage.log.TimestampOffsetResult;
import java.util.ArrayList;
import java.util.List;

/**
 * ListOffsets 请求处理器。
 */
public class ListOffsetsHandler implements ApiHandler {

    private static final long LATEST_TIMESTAMP = -1L;
    private static final long EARLIEST_TIMESTAMP = -2L;

    private final LogManager logManager;

    public ListOffsetsHandler(LogManager logManager) {
        this.logManager = logManager;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.LIST_OFFSETS;
    }

    @Override
    public ResponseContext handle(RequestContext requestContext) {
        if (requestContext.getApiVersion() != 0) {
            return ResponseContext.builder()
                    .requestContext(requestContext)
                    .apiKey(ApiKey.LIST_OFFSETS)
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
        if (!(requestContext.getRequestBody() instanceof ListOffsetsRequestBody requestBody)) {
            if (requestContext.getRequestBody() instanceof OpaqueRequestBody) {
                return ResponseContext.builder()
                        .requestContext(requestContext)
                        .apiKey(ApiKey.LIST_OFFSETS)
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
            throw new IllegalStateException("Unexpected request body type for list offsets request");
        }

        List<ListOffsetsTopicResponse> topicResponses = new ArrayList<>();
        for (ListOffsetsTopicRequest topic : requestBody.topics()) {
            List<ListOffsetsPartitionResponse> partitionResponses = new ArrayList<>();
            for (ListOffsetsPartitionRequest partition : topic.partitions()) {
                if (!logManager.containsTopic(topic.topic())
                        || !logManager.partitions(topic.topic()).contains(partition.partition())) {
                    partitionResponses.add(
                            new ListOffsetsPartitionResponse(
                                    partition.partition(),
                                    ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                                    0,
                                    0,
                                    0,
                                    List.of()));
                    continue;
                }
                int currentLeaderEpoch = logManager.leaderEpoch(topic.topic(), partition.partition());
                if (partition.currentLeaderEpoch() >= 0
                        && partition.currentLeaderEpoch() != currentLeaderEpoch) {
                    partitionResponses.add(
                            new ListOffsetsPartitionResponse(
                                    partition.partition(),
                                    ErrorCode.NOT_LEADER_OR_FOLLOWER,
                                    currentLeaderEpoch,
                                    0,
                                    0,
                                    List.of()));
                    continue;
                }
                if (partition.maxNumOffsets() <= 0) {
                    partitionResponses.add(
                            new ListOffsetsPartitionResponse(
                                    partition.partition(),
                                    ErrorCode.INVALID_REQUEST,
                                    currentLeaderEpoch,
                                    0,
                                    0,
                                    List.of()));
                    continue;
                }
                TimestampOffsetResult result;
                List<Long> offsets;
                if (partition.timestamp() == LATEST_TIMESTAMP) {
                    long latestOffset = logManager.logEndOffset(topic.topic(), partition.partition());
                    result =
                            new TimestampOffsetResult(
                                    latestOffset,
                                    System.currentTimeMillis());
                    offsets = List.of(latestOffset);
                } else if (partition.timestamp() == EARLIEST_TIMESTAMP) {
                    long earliestOffset = logManager.logStartOffset(topic.topic(), partition.partition());
                    result =
                            new TimestampOffsetResult(earliestOffset, 0);
                    offsets =
                            logManager.listOffsetsForTimestamp(
                                    topic.topic(),
                                    partition.partition(),
                                    EARLIEST_TIMESTAMP,
                                    partition.maxNumOffsets());
                } else {
                    result =
                            logManager.findOffsetByTimestamp(
                                    topic.topic(), partition.partition(), partition.timestamp());
                    offsets =
                            logManager.listOffsetsForTimestamp(
                                    topic.topic(),
                                    partition.partition(),
                                    partition.timestamp(),
                                    partition.maxNumOffsets());
                    if (offsets.isEmpty()) {
                        partitionResponses.add(
                                new ListOffsetsPartitionResponse(
                                        partition.partition(),
                                        ErrorCode.OFFSET_OUT_OF_RANGE,
                                        currentLeaderEpoch,
                                        partition.timestamp(),
                                        logManager.logEndOffset(topic.topic(), partition.partition()),
                                        List.of()));
                        continue;
                    }
                }
                partitionResponses.add(
                        new ListOffsetsPartitionResponse(
                                partition.partition(),
                                ErrorCode.NONE,
                                currentLeaderEpoch,
                                result.timestamp(),
                                result.offset(),
                                offsets));
            }
            topicResponses.add(new ListOffsetsTopicResponse(topic.topic(), partitionResponses));
        }

        return ResponseContext.builder()
                .requestContext(requestContext)
                .apiKey(ApiKey.LIST_OFFSETS)
                .apiVersion((short) 0)
                .responseHeader(
                        new ResponseHeader(
                                requestContext.getCorrelationId(), (short) 2, ErrorCode.NONE, 0))
                .responseBody(new ListOffsetsResponseBody(topicResponses))
                .build();
    }
}
