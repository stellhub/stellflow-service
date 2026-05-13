package io.github.stellhub.stellflow.server.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.FetchPartitionRequest;
import io.github.stellhub.stellflow.network.protocol.FetchPartitionResponse;
import io.github.stellhub.stellflow.network.protocol.FetchRequestBody;
import io.github.stellhub.stellflow.network.protocol.FetchResponseBody;
import io.github.stellhub.stellflow.network.protocol.FetchTopicRequest;
import io.github.stellhub.stellflow.controller.replica.ReplicaFollowerApplier;
import io.github.stellhub.stellflow.controller.replica.ReplicaPayloadCodec;
import io.github.stellhub.stellflow.storage.log.LogManager;
import io.github.stellhub.stellflow.storage.log.LogStorageConfig;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * FetchHandler 测试。
 */
class FetchHandlerTest {

    @TempDir private Path tempDir;

    /**
     * 验证 consumer fetch 会返回 records 的零拷贝文件区间。
     */
    @Test
    void shouldAttachZeroCopyRecordsForConsumerFetch() {
        LogStorageConfig config =
                LogStorageConfig.builder()
                        .rootDir(tempDir)
                        .segmentBytes(1024)
                        .indexIntervalBytes(1)
                        .retentionSegments(8)
                        .retentionMs(7L * 24 * 60 * 60 * 1000)
                        .retentionBytes(1024 * 1024)
                        .build();
        try (LogManager logManager = new LogManager(config)) {
            logManager.append("consumer-fetch", 0, "payload".getBytes(StandardCharsets.UTF_8));
            FetchHandler fetchHandler = new FetchHandler(logManager);
            RequestContext requestContext =
                    RequestContext.builder()
                            .apiKey(ApiKey.FETCH)
                            .apiVersion((short) 0)
                            .correlationId(7)
                            .requestBody(
                                    new FetchRequestBody(
                                            -1,
                                            500,
                                            1,
                                            4096,
                                            (byte) 0,
                                            0,
                                            List.of(
                                                    new FetchTopicRequest(
                                                            "consumer-fetch",
                                                            List.of(
                                                                    new FetchPartitionRequest(
                                                                            0, 0, 0, 0, 4096))))))
                            .build();

            ResponseContext responseContext = fetchHandler.handle(requestContext);
            FetchPartitionResponse partitionResponse =
                    ((FetchResponseBody) responseContext.getResponseBody())
                            .responses()
                            .get(0)
                            .partitions()
                            .get(0);

            assertEquals(0, partitionResponse.records().length);
            assertFalse(responseContext.getFetchRecordsFileRegions().isEmpty());
            FetchRecordsFileRegion fileRegion = responseContext.getFetchRecordsFileRegions().get(0);
            assertEquals("consumer-fetch", fileRegion.topic());
            assertEquals(0, fileRegion.partition());
            assertEquals(7, fileRegion.readableBytes());
        }
    }

    /**
     * 验证 replica fetch 会推进高水位。
     */
    @Test
    void shouldAdvanceHighWatermarkAfterReplicaFetch() {
        LogStorageConfig config =
                LogStorageConfig.builder()
                        .rootDir(tempDir)
                        .segmentBytes(1024)
                        .indexIntervalBytes(1)
                        .retentionSegments(8)
                        .retentionMs(7L * 24 * 60 * 60 * 1000)
                        .retentionBytes(1024 * 1024)
                        .build();
        try (LogManager logManager = new LogManager(config)) {
            logManager.updateReplicaTopology("replica-fetch", 0, 0, List.of(0, 1), List.of(0, 1));
            logManager.append("replica-fetch", 0, "r0".getBytes(StandardCharsets.UTF_8));
            logManager.append("replica-fetch", 0, "r1".getBytes(StandardCharsets.UTF_8));
            assertEquals(0L, logManager.highWatermark("replica-fetch", 0));

            FetchHandler fetchHandler = new FetchHandler(logManager);
            RequestContext requestContext =
                    RequestContext.builder()
                            .apiKey(ApiKey.FETCH)
                            .apiVersion((short) 0)
                            .correlationId(1)
                            .requestBody(
                                    new FetchRequestBody(
                                            1,
                                            500,
                                            1,
                                            4096,
                                            (byte) 0,
                                            0,
                                            List.of(
                                                    new FetchTopicRequest(
                                                            "replica-fetch",
                                                            List.of(
                                                                    new FetchPartitionRequest(
                                                                            0, 0, 0, 0, 4096))))))
                            .build();

            ResponseContext responseContext = fetchHandler.handle(requestContext);
            FetchResponseBody responseBody = (FetchResponseBody) responseContext.getResponseBody();
            assertEquals(2L, logManager.highWatermark("replica-fetch", 0));
            var entries =
                    ReplicaPayloadCodec.decode(
                            responseBody.responses().get(0).partitions().get(0).records());
            assertEquals(2, entries.size());
            assertEquals("r0", new String(entries.get(0).records(), StandardCharsets.UTF_8));
            assertEquals("r1", new String(entries.get(1).records(), StandardCharsets.UTF_8));
        }
    }

    /**
     * 验证 follower 可以将 replica fetch 结果追加到本地日志。
     */
    @Test
    void shouldAppendReplicaFetchPayloadOnFollower() {
        LogStorageConfig leaderConfig =
                LogStorageConfig.builder()
                        .rootDir(tempDir.resolve("leader"))
                        .segmentBytes(1024)
                        .indexIntervalBytes(1)
                        .retentionSegments(8)
                        .retentionMs(7L * 24 * 60 * 60 * 1000)
                        .retentionBytes(1024 * 1024)
                        .build();
        LogStorageConfig followerConfig =
                LogStorageConfig.builder()
                        .rootDir(tempDir.resolve("follower"))
                        .segmentBytes(1024)
                        .indexIntervalBytes(1)
                        .retentionSegments(8)
                        .retentionMs(7L * 24 * 60 * 60 * 1000)
                        .retentionBytes(1024 * 1024)
                        .build();
        try (LogManager leaderLogManager = new LogManager(leaderConfig);
                LogManager followerLogManager = new LogManager(followerConfig)) {
            leaderLogManager.updateReplicaTopology("replica-copy", 0, 0, List.of(0, 1), List.of(0, 1));
            leaderLogManager.append("replica-copy", 0, "a".getBytes(StandardCharsets.UTF_8));
            leaderLogManager.append("replica-copy", 0, "b".getBytes(StandardCharsets.UTF_8));

            FetchHandler fetchHandler = new FetchHandler(leaderLogManager);
            RequestContext requestContext =
                    RequestContext.builder()
                            .apiKey(ApiKey.FETCH)
                            .apiVersion((short) 0)
                            .correlationId(2)
                            .requestBody(
                                    new FetchRequestBody(
                                            1,
                                            500,
                                            1,
                                            4096,
                                            (byte) 0,
                                            0,
                                            List.of(
                                                    new FetchTopicRequest(
                                                            "replica-copy",
                                                            List.of(
                                                                    new FetchPartitionRequest(
                                                                            0, 0, 0, 0, 4096))))))
                            .build();

            ResponseContext responseContext = fetchHandler.handle(requestContext);
            FetchPartitionResponse partitionResponse =
                    ((FetchResponseBody) responseContext.getResponseBody())
                            .responses()
                            .get(0)
                            .partitions()
                            .get(0);

            ReplicaFollowerApplier applier = new ReplicaFollowerApplier(followerLogManager);
            applier.apply("replica-copy", 0, 0, partitionResponse);

            assertEquals(
                    "ab",
                    new String(
                            followerLogManager.read("replica-copy", 0, 0, 4096).records(),
                            StandardCharsets.UTF_8));
        }
    }
}
