package io.github.stellhub.stellflow.storage.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.stellhub.stellflow.controller.replica.ControllerReplicaCoordinator;
import io.github.stellhub.stellflow.controller.replica.PartitionControlCommand;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * storage.log 层测试。
 */
class LogManagerTest {

    @TempDir private Path tempDir;

    /**
     * 验证 segment rolling 后仍可跨段读取。
     */
    @Test
    void shouldRollSegmentsAndReadAcrossSegments() throws IOException {
        LogStorageConfig config =
                LogStorageConfig.builder()
                        .rootDir(tempDir)
                        .segmentBytes(64)
                        .indexIntervalBytes(1)
                        .build();
        try (LogManager logManager = new LogManager(config)) {
            logManager.append("orders", 0, "segment-a".getBytes(StandardCharsets.UTF_8));
            logManager.append("orders", 0, "segment-b".getBytes(StandardCharsets.UTF_8));
            logManager.append("orders", 0, "segment-c".getBytes(StandardCharsets.UTF_8));

            LogReadResult readResult = logManager.read("orders", 0, 0, 4096);
            assertEquals(
                    "segment-asegment-bsegment-c",
                    new String(readResult.records(), StandardCharsets.UTF_8));
            assertEquals(3L, readResult.highWatermark());
        }

        Path partitionDir = tempDir.resolve("orders-0");
        long logSegmentCount;
        long offsetIndexCount;
        long timeIndexCount;
        try (var stream = Files.list(partitionDir)) {
            logSegmentCount = stream.filter(path -> path.toString().endsWith(".log")).count();
        }
        try (var stream = Files.list(partitionDir)) {
            offsetIndexCount = stream.filter(path -> path.toString().endsWith(".index")).count();
        }
        try (var stream = Files.list(partitionDir)) {
            timeIndexCount = stream.filter(path -> path.toString().endsWith(".timeindex")).count();
        }
        assertTrue(logSegmentCount >= 2);
        assertTrue(offsetIndexCount >= 2);
        assertTrue(timeIndexCount >= 2);
    }

    /**
     * 验证 FileRegion 读取视图只暴露 record payload 区间。
     */
    @Test
    void shouldReadFileRegionsAcrossSegments() throws IOException {
        LogStorageConfig config =
                LogStorageConfig.builder()
                        .rootDir(tempDir)
                        .segmentBytes(64)
                        .indexIntervalBytes(1)
                        .build();
        try (LogManager logManager = new LogManager(config)) {
            logManager.append("file-region", 0, "segment-a".getBytes(StandardCharsets.UTF_8));
            logManager.append("file-region", 0, "segment-b".getBytes(StandardCharsets.UTF_8));
            logManager.append("file-region", 0, "segment-c".getBytes(StandardCharsets.UTF_8));

            LogFileRegionReadResult readResult =
                    logManager.readFileRegions("file-region", 0, 0, 4096);

            assertEquals(3L, readResult.highWatermark());
            assertEquals(3L, readResult.nextFetchOffset());
            assertEquals(27, readResult.readableBytes());
            assertEquals(3, readResult.fileRegions().size());
            assertEquals(
                    "segment-asegment-bsegment-c",
                    new String(readRegions(readResult.fileRegions()), StandardCharsets.UTF_8));
            assertTrue(readResult.fileRegions().stream().allMatch(region -> region.position() > 0));
        }
    }

    /**
     * 验证 FileRegion 读取视图保持 offset 与 maxBytes 语义。
     */
    @Test
    void shouldLimitFileRegionsByOffsetAndMaxBytes() throws IOException {
        LogStorageConfig config =
                LogStorageConfig.builder()
                        .rootDir(tempDir)
                        .segmentBytes(1024)
                        .indexIntervalBytes(1)
                        .build();
        try (LogManager logManager = new LogManager(config)) {
            logManager.append("file-region-limit", 0, "first".getBytes(StandardCharsets.UTF_8));
            logManager.append("file-region-limit", 0, "second".getBytes(StandardCharsets.UTF_8));
            logManager.append("file-region-limit", 0, "third".getBytes(StandardCharsets.UTF_8));

            LogFileRegionReadResult readResult =
                    logManager.readFileRegions("file-region-limit", 0, 1, 6);

            assertEquals(2L, readResult.nextFetchOffset());
            assertEquals(6, readResult.readableBytes());
            assertEquals(
                    "second", new String(readRegions(readResult.fileRegions()), StandardCharsets.UTF_8));
        }
    }

    /**
     * 验证恢复时会截断损坏尾部并重建索引。
     */
    @Test
    void shouldRecoverAndTruncateCorruptedTail() throws IOException {
        LogStorageConfig config =
                LogStorageConfig.builder()
                        .rootDir(tempDir)
                        .segmentBytes(1024)
                        .indexIntervalBytes(1)
                        .build();
        Path partitionDir = tempDir.resolve("recovery-0");
        Path logFile = partitionDir.resolve("00000000000000000000.log");
        try (LogManager logManager = new LogManager(config)) {
            logManager.append("recovery", 0, "first".getBytes(StandardCharsets.UTF_8));
            logManager.append("recovery", 0, "second".getBytes(StandardCharsets.UTF_8));
        }

        long validSize = Files.size(logFile);
        Files.write(logFile, new byte[] {1, 2, 3, 4, 5, 6, 7}, StandardOpenOption.APPEND);
        assertTrue(Files.size(logFile) > validSize);

        try (LogManager logManager = new LogManager(config)) {
            LogReadResult readResult = logManager.read("recovery", 0, 0, 4096);
            assertEquals("firstsecond", new String(readResult.records(), StandardCharsets.UTF_8));
        }

        assertEquals(validSize, Files.size(logFile));
        assertTrue(Files.exists(partitionDir.resolve("00000000000000000000.index")));
        assertTrue(Files.exists(partitionDir.resolve("00000000000000000000.timeindex")));
    }

    /**
     * 验证 checkpoint 会持久化高水位与 leader epoch。
     */
    @Test
    void shouldPersistCheckpointHighWatermarkAndLeaderEpoch() throws IOException {
        LogStorageConfig config =
                LogStorageConfig.builder()
                        .rootDir(tempDir)
                        .segmentBytes(1024)
                        .indexIntervalBytes(1)
                        .build();
        try (LogManager logManager = new LogManager(config)) {
            logManager.updateLeaderEpoch("checkpoint", 0, 3);
            logManager.append("checkpoint", 0, "a".getBytes(StandardCharsets.UTF_8));
            logManager.append("checkpoint", 0, "b".getBytes(StandardCharsets.UTF_8));

            assertEquals(3, logManager.leaderEpoch("checkpoint", 0));
            assertEquals(2L, logManager.highWatermark("checkpoint", 0));
        }

        Path checkpointFile = tempDir.resolve("checkpoint-0").resolve("log.checkpoint");
        assertTrue(Files.exists(checkpointFile));
        Properties properties = new Properties();
        try (var inputStream = Files.newInputStream(checkpointFile)) {
            properties.load(inputStream);
        }
        assertEquals("3", properties.getProperty("leaderEpoch"));
        assertEquals("2", properties.getProperty("highWatermark"));
        assertEquals("2", properties.getProperty("logEndOffset"));

        try (LogManager reopened = new LogManager(config)) {
            assertEquals(3, reopened.leaderEpoch("checkpoint", 0));
            assertEquals(2L, reopened.highWatermark("checkpoint", 0));
            assertEquals(
                    "ab",
                    new String(
                            reopened.read("checkpoint", 0, 0, 4096).records(),
                            StandardCharsets.UTF_8));
        }
    }

    /**
     * 验证时间索引可返回正式查询结果。
     */
    @Test
    void shouldFindOffsetByTimestamp() throws Exception {
        LogStorageConfig config =
                LogStorageConfig.builder()
                        .rootDir(tempDir)
                        .segmentBytes(1024)
                        .indexIntervalBytes(1)
                        .build();
        try (LogManager logManager = new LogManager(config)) {
            logManager.append("timeline", 0, "t0".getBytes(StandardCharsets.UTF_8));
            Thread.sleep(5);
            long middle = System.currentTimeMillis();
            Thread.sleep(5);
            logManager.append("timeline", 0, "t1".getBytes(StandardCharsets.UTF_8));
            TimestampOffsetResult result = logManager.findOffsetByTimestamp("timeline", 0, middle);
            assertEquals(1L, result.offset());
            assertTrue(result.timestamp() >= middle);
        }
    }

    /**
     * 验证 segment 保留策略会推进 logStartOffset。
     */
    @Test
    void shouldAdvanceLogStartOffsetWhenRetentionDeletesOldSegments() {
        LogStorageConfig config =
                LogStorageConfig.builder()
                        .rootDir(tempDir)
                        .segmentBytes(64)
                        .indexIntervalBytes(1)
                        .retentionSegments(2)
                        .build();
        try (LogManager logManager = new LogManager(config)) {
            logManager.append("retention", 0, "segment-0".getBytes(StandardCharsets.UTF_8));
            logManager.append("retention", 0, "segment-1".getBytes(StandardCharsets.UTF_8));
            logManager.append("retention", 0, "segment-2".getBytes(StandardCharsets.UTF_8));
            logManager.append("retention", 0, "segment-3".getBytes(StandardCharsets.UTF_8));

            assertTrue(logManager.logStartOffset("retention", 0) > 0);
            LogReadResult readResult = logManager.read("retention", 0, 0, 4096);
            assertTrue(new String(readResult.records(), StandardCharsets.UTF_8).contains("segment-"));
        }
    }

    /**
     * 验证按时间的 retention 会删除过旧 segment。
     */
    @Test
    void shouldDeleteExpiredSegmentsByRetentionMs() throws Exception {
        LogStorageConfig config =
                LogStorageConfig.builder()
                        .rootDir(tempDir)
                        .segmentBytes(64)
                        .indexIntervalBytes(1)
                        .retentionSegments(8)
                        .retentionMs(5)
                        .retentionBytes(1024 * 1024)
                        .build();
        try (LogManager logManager = new LogManager(config)) {
            logManager.append("retention-time", 0, "old-segment".getBytes(StandardCharsets.UTF_8));
            Thread.sleep(10);
            logManager.append("retention-time", 0, "new-segment".getBytes(StandardCharsets.UTF_8));
            logManager.append("retention-time", 0, "tail".getBytes(StandardCharsets.UTF_8));

            assertTrue(logManager.logStartOffset("retention-time", 0) > 0);
        }
    }

    /**
     * 验证按总字节数的 retention 会删除最老 segment。
     */
    @Test
    void shouldDeleteOldSegmentsByRetentionBytes() {
        LogStorageConfig config =
                LogStorageConfig.builder()
                        .rootDir(tempDir)
                        .segmentBytes(64)
                        .indexIntervalBytes(1)
                        .retentionSegments(8)
                        .retentionMs(7L * 24 * 60 * 60 * 1000)
                        .retentionBytes(90)
                        .build();
        try (LogManager logManager = new LogManager(config)) {
            logManager.append("retention-bytes", 0, "segment-0".getBytes(StandardCharsets.UTF_8));
            logManager.append("retention-bytes", 0, "segment-1".getBytes(StandardCharsets.UTF_8));
            logManager.append("retention-bytes", 0, "segment-2".getBytes(StandardCharsets.UTF_8));
            logManager.append("retention-bytes", 0, "segment-3".getBytes(StandardCharsets.UTF_8));

            assertTrue(logManager.logStartOffset("retention-bytes", 0) > 0);
        }
    }

    /**
     * 验证按 offset 删除 segment 会推进 logStartOffset。
     */
    @Test
    void shouldDeleteSegmentsBeforeOffset() {
        LogStorageConfig config =
                LogStorageConfig.builder()
                        .rootDir(tempDir)
                        .segmentBytes(64)
                        .indexIntervalBytes(1)
                        .retentionSegments(8)
                        .build();
        try (LogManager logManager = new LogManager(config)) {
            logManager.append("cleanup", 0, "segment-a".getBytes(StandardCharsets.UTF_8));
            logManager.append("cleanup", 0, "segment-b".getBytes(StandardCharsets.UTF_8));
            logManager.append("cleanup", 0, "segment-c".getBytes(StandardCharsets.UTF_8));
            logManager.deleteSegmentsBeforeOffset("cleanup", 0, 2);

            assertEquals(2L, logManager.logStartOffset("cleanup", 0));
            LogReadResult readResult = logManager.read("cleanup", 0, 0, 4096);
            assertEquals("segment-c", new String(readResult.records(), StandardCharsets.UTF_8));
        }
    }

    /**
     * 验证 follower 复制进度和 ISR 会共同推进高水位。
     */
    @Test
    void shouldAdvanceHighWatermarkByIsrProgress() {
        LogStorageConfig config =
                LogStorageConfig.builder()
                        .rootDir(tempDir)
                        .segmentBytes(1024)
                        .indexIntervalBytes(1)
                        .build();
        try (LogManager logManager = new LogManager(config)) {
            logManager.updateReplicaTopology("replica", 0, 0, List.of(0, 1, 2), List.of(0, 1, 2));
            logManager.append("replica", 0, "r0".getBytes(StandardCharsets.UTF_8));
            logManager.append("replica", 0, "r1".getBytes(StandardCharsets.UTF_8));

            assertEquals(0L, logManager.highWatermark("replica", 0));

            logManager.updateReplicaFetchOffset("replica", 0, 1, 1);
            assertEquals(0L, logManager.highWatermark("replica", 0));

            logManager.updateReplicaFetchOffset("replica", 0, 2, 1);
            assertEquals(1L, logManager.highWatermark("replica", 0));

            logManager.updateReplicaFetchOffset("replica", 0, 1, 2);
            logManager.updateReplicaFetchOffset("replica", 0, 2, 2);
            assertEquals(2L, logManager.highWatermark("replica", 0));
        }
    }

    /**
     * 验证 controller/replica 联动时会推进 epoch 并截断日志。
     */
    @Test
    void shouldLinkLeaderEpochAndTruncationWithControllerReplicaCoordinator() {
        LogStorageConfig config =
                LogStorageConfig.builder()
                        .rootDir(tempDir)
                        .segmentBytes(1024)
                        .indexIntervalBytes(1)
                        .build();
        try (LogManager logManager = new LogManager(config)) {
            logManager.append("controller", 0, "c0".getBytes(StandardCharsets.UTF_8));
            logManager.append("controller", 0, "c1".getBytes(StandardCharsets.UTF_8));
            logManager.append("controller", 0, "c2".getBytes(StandardCharsets.UTF_8));

            ControllerReplicaCoordinator coordinator = new ControllerReplicaCoordinator(logManager);
            coordinator.apply(
                    new PartitionControlCommand(
                            "controller",
                            0,
                            1,
                            5,
                            List.of(0, 1, 2),
                            List.of(0, 1),
                            null,
                            2L,
                            false));

            assertEquals(5, logManager.leaderEpoch("controller", 0));
            assertEquals(1, logManager.leaderId("controller", 0));
            assertEquals(List.of(0, 1, 2), logManager.replicaNodes("controller", 0));
            assertEquals(List.of(0, 1), logManager.isrNodes("controller", 0));
            assertEquals(
                    "c0c1",
                    new String(
                            logManager.read("controller", 0, 0, 4096).records(),
                            StandardCharsets.UTF_8));
        }
    }

    /**
     * 验证可按 leader epoch 分叉点做细粒度截断。
     */
    @Test
    void shouldTruncateByLeaderEpochBoundary() {
        LogStorageConfig config =
                LogStorageConfig.builder()
                        .rootDir(tempDir)
                        .segmentBytes(1024)
                        .indexIntervalBytes(1)
                        .build();
        try (LogManager logManager = new LogManager(config)) {
            logManager.updateLeaderEpoch("epoch-truncate", 0, 0);
            logManager.append("epoch-truncate", 0, "e0-a".getBytes(StandardCharsets.UTF_8));
            logManager.append("epoch-truncate", 0, "e0-b".getBytes(StandardCharsets.UTF_8));
            logManager.updateLeaderEpoch("epoch-truncate", 0, 1);
            logManager.append("epoch-truncate", 0, "e1-a".getBytes(StandardCharsets.UTF_8));
            logManager.updateLeaderEpoch("epoch-truncate", 0, 2);
            logManager.append("epoch-truncate", 0, "e2-a".getBytes(StandardCharsets.UTF_8));

            logManager.truncateToLeaderEpoch("epoch-truncate", 0, 1);

            assertEquals(3L, logManager.logEndOffset("epoch-truncate", 0));
            assertEquals(
                    "e0-ae0-be1-a",
                    new String(
                            logManager.read("epoch-truncate", 0, 0, 4096).records(),
                            StandardCharsets.UTF_8));
        }
    }

    /**
     * 验证 controller 命令可按 leader epoch 分叉点截断。
     */
    @Test
    void shouldApplyLeaderEpochBoundaryTruncationFromCoordinator() {
        LogStorageConfig config =
                LogStorageConfig.builder()
                        .rootDir(tempDir)
                        .segmentBytes(1024)
                        .indexIntervalBytes(1)
                        .build();
        try (LogManager logManager = new LogManager(config)) {
            logManager.updateLeaderEpoch("epoch-coordinator", 0, 0);
            logManager.append("epoch-coordinator", 0, "b0".getBytes(StandardCharsets.UTF_8));
            logManager.updateLeaderEpoch("epoch-coordinator", 0, 1);
            logManager.append("epoch-coordinator", 0, "b1".getBytes(StandardCharsets.UTF_8));
            logManager.updateLeaderEpoch("epoch-coordinator", 0, 2);
            logManager.append("epoch-coordinator", 0, "b2".getBytes(StandardCharsets.UTF_8));

            ControllerReplicaCoordinator coordinator = new ControllerReplicaCoordinator(logManager);
            coordinator.apply(
                    new PartitionControlCommand(
                            "epoch-coordinator",
                            0,
                            1,
                            2,
                            List.of(0, 1),
                            List.of(0, 1),
                            1,
                            null,
                            false));

            assertEquals(2L, logManager.logEndOffset("epoch-coordinator", 0));
            assertEquals(
                    "b0b1",
                    new String(
                            logManager.read("epoch-coordinator", 0, 0, 4096).records(),
                            StandardCharsets.UTF_8));
        }
    }

    private byte[] readRegions(List<LogFileRegion> fileRegions) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (LogFileRegion fileRegion : fileRegions) {
            ByteBuffer buffer = ByteBuffer.allocate(Math.toIntExact(fileRegion.count()));
            try (FileChannel channel = FileChannel.open(fileRegion.file(), StandardOpenOption.READ)) {
                channel.position(fileRegion.position());
                while (buffer.hasRemaining() && channel.read(buffer) >= 0) {
                    // Read until the requested region is materialized for assertion.
                }
            }
            outputStream.write(buffer.array());
        }
        return outputStream.toByteArray();
    }
}
