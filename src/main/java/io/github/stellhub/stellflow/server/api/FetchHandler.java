package io.github.stellhub.stellflow.server.api;

import io.github.stellhub.stellflow.network.protocol.AbortedTransaction;
import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.EmptyResponseBody;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.network.protocol.FetchPartitionRequest;
import io.github.stellhub.stellflow.network.protocol.FetchPartitionResponse;
import io.github.stellhub.stellflow.network.protocol.FetchRequestBody;
import io.github.stellhub.stellflow.network.protocol.FetchResponseBody;
import io.github.stellhub.stellflow.network.protocol.FetchTopicRequest;
import io.github.stellhub.stellflow.network.protocol.FetchTopicResponse;
import io.github.stellhub.stellflow.network.protocol.OpaqueRequestBody;
import io.github.stellhub.stellflow.network.protocol.ResponseHeader;
import io.github.stellhub.stellflow.observability.metrics.StellflowMetrics;
import io.github.stellhub.stellflow.controller.replica.ReplicaPayloadCodec;
import io.github.stellhub.stellflow.server.runtime.PartitionReadResult;
import io.github.stellhub.stellflow.server.runtime.ReplicaManager;
import io.github.stellhub.stellflow.storage.log.LogFileRegion;
import io.github.stellhub.stellflow.storage.log.LogFileRegionReadResult;
import io.github.stellhub.stellflow.storage.log.LogManager;
import io.github.stellhub.stellflow.storage.log.ReplicaLogReadResult;
import java.util.ArrayList;
import java.util.List;

/**
 * Fetch 请求处理器。
 */
public class FetchHandler implements ApiHandler {

    private final LogManager logManager;
    private final ReplicaManager replicaManager;
    private final StellflowMetrics metrics;

    public FetchHandler(LogManager logManager) {
        this.logManager = logManager;
        this.replicaManager = null;
        this.metrics = StellflowMetrics.global();
    }

    public FetchHandler(ReplicaManager replicaManager) {
        this(replicaManager, StellflowMetrics.global());
    }

    public FetchHandler(ReplicaManager replicaManager, StellflowMetrics metrics) {
        this.logManager = null;
        this.replicaManager = replicaManager;
        this.metrics = metrics;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.FETCH;
    }

    @Override
    public ResponseContext handle(RequestContext requestContext) {
        if (requestContext.getApiVersion() != 0) {
            return ResponseContext.builder()
                    .requestContext(requestContext)
                    .apiKey(ApiKey.FETCH)
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

        if (!(requestContext.getRequestBody() instanceof FetchRequestBody fetchRequestBody)) {
            if (requestContext.getRequestBody() instanceof OpaqueRequestBody) {
                return ResponseContext.builder()
                        .requestContext(requestContext)
                        .apiKey(ApiKey.FETCH)
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
            throw new IllegalStateException("Unexpected request body type for fetch request");
        }

        List<FetchTopicResponse> responses = new ArrayList<>();
        List<FetchRecordsFileRegion> fetchRecordsFileRegions = new ArrayList<>();
        for (FetchTopicRequest topicRequest : fetchRequestBody.topicPartitions()) {
            List<FetchPartitionResponse> partitionResponses = new ArrayList<>();
            for (FetchPartitionRequest partitionRequest : topicRequest.partitions()) {
                if (fetchRequestBody.replicaId() >= 0) {
                    partitionResponses.add(
                            handleReplicaFetch(fetchRequestBody, topicRequest, partitionRequest));
                    continue;
                }
                partitionResponses.add(
                        handleConsumerFetch(topicRequest, partitionRequest, fetchRecordsFileRegions));
            }
            responses.add(new FetchTopicResponse(topicRequest.topic(), partitionResponses));
        }

        return ResponseContext.builder()
                .requestContext(requestContext)
                .apiKey(ApiKey.FETCH)
                .apiVersion((short) 0)
                .responseHeader(
                        new ResponseHeader(
                                requestContext.getCorrelationId(), (short) 2, ErrorCode.NONE, 0))
                .responseBody(new FetchResponseBody(fetchRequestBody.sessionId(), responses))
                .fetchRecordsFileRegions(fetchRecordsFileRegions)
                .build();
    }

    /**
     * 处理 consumer fetch。
     */
    private FetchPartitionResponse handleConsumerFetch(
            FetchTopicRequest topicRequest,
            FetchPartitionRequest partitionRequest,
            List<FetchRecordsFileRegion> fetchRecordsFileRegions) {
        long startMs = System.currentTimeMillis();
        if (replicaManager != null) {
            PartitionReadResult fetchResult =
                    replicaManager.readConsumer(
                            topicRequest.topic(),
                            partitionRequest.partition(),
                            partitionRequest.fetchOffset(),
                            partitionRequest.partitionMaxBytes());
            addFetchRecordsFileRegions(
                    fetchRecordsFileRegions,
                    topicRequest.topic(),
                    partitionRequest.partition(),
                    fetchResult.recordFileRegions());
            recordFetch(
                    "consumer",
                    topicRequest.topic(),
                    partitionRequest.partition(),
                    fetchResult.errorCode(),
                    recordsBytes(fetchResult.recordFileRegions(), fetchResult.records()),
                    startMs);
            return new FetchPartitionResponse(
                    partitionRequest.partition(),
                    fetchResult.errorCode(),
                    fetchResult.highWatermark(),
                    fetchResult.logStartOffset(),
                    fetchResult.lastStableOffset(),
                    List.<AbortedTransaction>of(),
                    fetchResult.records());
        }
        LogFileRegionReadResult fetchResult =
                logManager.readFileRegions(
                        topicRequest.topic(),
                        partitionRequest.partition(),
                        partitionRequest.fetchOffset(),
                        partitionRequest.partitionMaxBytes());
        if (fetchResult == null) {
            recordFetch(
                    "consumer",
                    topicRequest.topic(),
                    partitionRequest.partition(),
                    ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                    0,
                    startMs);
            return new FetchPartitionResponse(
                    partitionRequest.partition(),
                    ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                    0,
                    0,
                    0,
                    List.of(),
                    new byte[0]);
        }
        addFetchRecordsFileRegions(
                fetchRecordsFileRegions,
                topicRequest.topic(),
                partitionRequest.partition(),
                fetchResult.fileRegions());
        recordFetch(
                "consumer",
                topicRequest.topic(),
                partitionRequest.partition(),
                ErrorCode.NONE,
                fetchResult.readableBytes(),
                startMs);
        return new FetchPartitionResponse(
                partitionRequest.partition(),
                ErrorCode.NONE,
                fetchResult.highWatermark(),
                fetchResult.logStartOffset(),
                fetchResult.lastStableOffset(),
                List.<AbortedTransaction>of(),
                new byte[0]);
    }

    private void addFetchRecordsFileRegions(
            List<FetchRecordsFileRegion> fetchRecordsFileRegions,
            String topic,
            int partition,
            List<LogFileRegion> fileRegions) {
        if (fileRegions == null || fileRegions.isEmpty()) {
            return;
        }
        List<ZeroCopyFileRegion> zeroCopyFileRegions =
                fileRegions.stream()
                        .map(region -> new ZeroCopyFileRegion(region.file(), region.position(), region.count()))
                        .toList();
        int readableBytes =
                Math.toIntExact(zeroCopyFileRegions.stream().mapToLong(ZeroCopyFileRegion::count).sum());
        fetchRecordsFileRegions.add(
                new FetchRecordsFileRegion(topic, partition, readableBytes, zeroCopyFileRegions));
    }

    /**
     * 处理 replica fetch。
     */
    private FetchPartitionResponse handleReplicaFetch(
            FetchRequestBody fetchRequestBody,
            FetchTopicRequest topicRequest,
            FetchPartitionRequest partitionRequest) {
        long startMs = System.currentTimeMillis();
        if (replicaManager != null) {
            PartitionReadResult fetchResult =
                    replicaManager.readReplica(
                            topicRequest.topic(),
                            partitionRequest.partition(),
                            fetchRequestBody.replicaId(),
                            partitionRequest.fetchOffset(),
                            partitionRequest.partitionMaxBytes());
            byte[] records = ReplicaPayloadCodec.encode(fetchResult.replicaEntries());
            recordFetch(
                    "replica",
                    topicRequest.topic(),
                    partitionRequest.partition(),
                    fetchResult.errorCode(),
                    records.length,
                    startMs);
            return new FetchPartitionResponse(
                    partitionRequest.partition(),
                    fetchResult.errorCode(),
                    fetchResult.highWatermark(),
                    fetchResult.logStartOffset(),
                    fetchResult.lastStableOffset(),
                    List.of(),
                    records);
        }
        ReplicaLogReadResult fetchResult =
                logManager.readReplica(
                        topicRequest.topic(),
                        partitionRequest.partition(),
                        partitionRequest.fetchOffset(),
                        partitionRequest.partitionMaxBytes());
        if (fetchResult == null) {
            recordFetch(
                    "replica",
                    topicRequest.topic(),
                    partitionRequest.partition(),
                    ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                    0,
                    startMs);
            return new FetchPartitionResponse(
                    partitionRequest.partition(),
                    ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                    0,
                    0,
                    0,
                    List.of(),
                    new byte[0]);
        }
        logManager.updateReplicaFetchOffset(
                topicRequest.topic(),
                partitionRequest.partition(),
                fetchRequestBody.replicaId(),
                fetchResult.nextFetchOffset());
        byte[] records = ReplicaPayloadCodec.encode(fetchResult.entries());
        recordFetch(
                "replica",
                topicRequest.topic(),
                partitionRequest.partition(),
                ErrorCode.NONE,
                records.length,
                startMs);
        return new FetchPartitionResponse(
                partitionRequest.partition(),
                ErrorCode.NONE,
                fetchResult.highWatermark(),
                fetchResult.logStartOffset(),
                fetchResult.lastStableOffset(),
                List.of(),
                records);
    }

    private void recordFetch(
            String fetchType,
            String topic,
            int partition,
            ErrorCode errorCode,
            long bytes,
            long startMs) {
        metrics.recordFetch(
                fetchType, topic, partition, errorCode, bytes, System.currentTimeMillis() - startMs);
    }

    private long recordsBytes(List<LogFileRegion> fileRegions, byte[] records) {
        long bytes = records == null ? 0 : records.length;
        for (LogFileRegion fileRegion : fileRegions) {
            bytes += fileRegion.count();
        }
        return bytes;
    }
}
