package io.github.stellhub.stellflow.server.api;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.stellhub.stellflow.network.protocol.ApiKey;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.network.protocol.ListOffsetsPartitionRequest;
import io.github.stellhub.stellflow.network.protocol.ListOffsetsRequestBody;
import io.github.stellhub.stellflow.network.protocol.ListOffsetsResponseBody;
import io.github.stellhub.stellflow.network.protocol.ListOffsetsTopicRequest;
import io.github.stellhub.stellflow.storage.log.LogManager;
import io.github.stellhub.stellflow.storage.log.LogStorageConfig;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * ListOffsetsHandler 测试。
 */
class ListOffsetsHandlerTest {

    @TempDir private Path tempDir;

    /**
     * 验证 leaderEpoch 不匹配时返回分区级错误。
     */
    @Test
    void shouldRejectMismatchedLeaderEpoch() {
        try (LogManager logManager = new LogManager(defaultConfig(tempDir))) {
            logManager.updateLeaderEpoch("epoch", 0, 3);
            logManager.append("epoch", 0, "a".getBytes(StandardCharsets.UTF_8));

            ListOffsetsHandler handler = new ListOffsetsHandler(logManager);
            ResponseContext responseContext =
                    handler.handle(
                            RequestContext.builder()
                                    .apiKey(ApiKey.LIST_OFFSETS)
                                    .apiVersion((short) 0)
                                    .correlationId(1)
                                    .requestBody(
                                            new ListOffsetsRequestBody(
                                                    -1,
                                                    (byte) 0,
                                                    List.of(
                                                            new ListOffsetsTopicRequest(
                                                                    "epoch",
                                                                    List.of(
                                                                            new ListOffsetsPartitionRequest(
                                                                                    0, 2, -2L, 1))))))
                                    .build());

            ListOffsetsResponseBody responseBody = (ListOffsetsResponseBody) responseContext.getResponseBody();
            assertEquals(
                    ErrorCode.NOT_LEADER_OR_FOLLOWER,
                    responseBody.topics().get(0).partitions().get(0).errorCode());
        }
    }

    /**
     * 验证 maxNumOffsets 可返回多个 offset。
     */
    @Test
    void shouldReturnMultipleOffsetsWhenMaxNumOffsetsIsGreaterThanOne() {
        try (LogManager logManager = new LogManager(defaultConfig(tempDir))) {
            logManager.append("offsets", 0, "a".getBytes(StandardCharsets.UTF_8));
            logManager.append("offsets", 0, "b".getBytes(StandardCharsets.UTF_8));
            logManager.append("offsets", 0, "c".getBytes(StandardCharsets.UTF_8));

            ListOffsetsHandler handler = new ListOffsetsHandler(logManager);
            ResponseContext responseContext =
                    handler.handle(
                            RequestContext.builder()
                                    .apiKey(ApiKey.LIST_OFFSETS)
                                    .apiVersion((short) 0)
                                    .correlationId(2)
                                    .requestBody(
                                            new ListOffsetsRequestBody(
                                                    -1,
                                                    (byte) 0,
                                                    List.of(
                                                            new ListOffsetsTopicRequest(
                                                                    "offsets",
                                                                    List.of(
                                                                            new ListOffsetsPartitionRequest(
                                                                                    0, 0, -2L, 3))))))
                                    .build());

            ListOffsetsResponseBody responseBody = (ListOffsetsResponseBody) responseContext.getResponseBody();
            assertEquals(
                    List.of(0L, 1L, 2L),
                    responseBody.topics().get(0).partitions().get(0).offsets());
        }
    }

    /**
     * 验证时间戳超出范围时返回 OFFSET_OUT_OF_RANGE。
     */
    @Test
    void shouldReturnOffsetOutOfRangeWhenTimestampIsBeyondNewestRecord() {
        try (LogManager logManager = new LogManager(defaultConfig(tempDir))) {
            logManager.append("range", 0, "a".getBytes(StandardCharsets.UTF_8));

            ListOffsetsHandler handler = new ListOffsetsHandler(logManager);
            ResponseContext responseContext =
                    handler.handle(
                            RequestContext.builder()
                                    .apiKey(ApiKey.LIST_OFFSETS)
                                    .apiVersion((short) 0)
                                    .correlationId(3)
                                    .requestBody(
                                            new ListOffsetsRequestBody(
                                                    -1,
                                                    (byte) 0,
                                                    List.of(
                                                            new ListOffsetsTopicRequest(
                                                                    "range",
                                                                    List.of(
                                                                            new ListOffsetsPartitionRequest(
                                                                                    0,
                                                                                    0,
                                                                                    System.currentTimeMillis()
                                                                                            + 60_000,
                                                                                    1))))))
                                    .build());

            ListOffsetsResponseBody responseBody = (ListOffsetsResponseBody) responseContext.getResponseBody();
            assertEquals(
                    ErrorCode.OFFSET_OUT_OF_RANGE,
                    responseBody.topics().get(0).partitions().get(0).errorCode());
        }
    }

    private static LogStorageConfig defaultConfig(Path rootDir) {
        return LogStorageConfig.builder()
                .rootDir(rootDir)
                .segmentBytes(1024)
                .indexIntervalBytes(1)
                .retentionSegments(8)
                .retentionMs(7L * 24 * 60 * 60 * 1000)
                .retentionBytes(1024 * 1024)
                .build();
    }
}
