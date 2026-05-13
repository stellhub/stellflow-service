package io.github.stellhub.stellflow.tools.benchmark;

import io.github.stellhub.stellflow.storage.log.LogManager;
import io.github.stellhub.stellflow.storage.log.LogReadResult;
import io.github.stellhub.stellflow.storage.log.LogStorageConfig;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * 独立 benchmark runner。
 */
public final class BenchmarkRunner {

    private BenchmarkRunner() {}

    /**
     * 运行本地存储 append/read 基线压测。
     */
    public static void main(String[] args) {
        Map<String, String> options = parseArgs(args);
        Path rootDir = Path.of(options.getOrDefault("rootDir", "data/benchmark-logs"));
        String topic = options.getOrDefault("topic", "benchmark-topic");
        int partitions = parsePositiveInt(options, "partitions", 4);
        int records = parsePositiveInt(options, "records", 100_000);
        int recordBytes = parsePositiveInt(options, "recordBytes", 1024);
        boolean flushEveryAppend = Boolean.parseBoolean(options.getOrDefault("flushEveryAppend", "false"));
        LogStorageConfig storageConfig =
                LogStorageConfig.builder()
                        .rootDir(rootDir)
                        .segmentBytes(parsePositiveInt(options, "segmentBytes", 32 * 1024 * 1024))
                        .indexIntervalBytes(4096)
                        .retentionSegments(128)
                        .retentionMs(7L * 24 * 60 * 60 * 1000)
                        .retentionBytes(128L * 1024 * 1024 * 1024)
                        .flushEveryAppend(flushEveryAppend)
                        .build();
        byte[] payload = payload(recordBytes);
        try (LogManager logManager = new LogManager(storageConfig)) {
            BenchmarkResult append = runAppend(logManager, topic, partitions, records, payload);
            BenchmarkResult read = runRead(logManager, topic, partitions, records, recordBytes);
            print(append);
            print(read);
        }
    }

    private static BenchmarkResult runAppend(
            LogManager logManager, String topic, int partitions, int records, byte[] payload) {
        long started = System.nanoTime();
        for (int index = 0; index < records; index++) {
            logManager.append(topic, index % partitions, payload);
        }
        long elapsed = System.nanoTime() - started;
        return BenchmarkResult.of("append", records, (long) records * payload.length, elapsed);
    }

    private static BenchmarkResult runRead(
            LogManager logManager, String topic, int partitions, int records, int recordBytes) {
        long bytes = 0;
        long readRecords = 0;
        long started = System.nanoTime();
        for (int partition = 0; partition < partitions; partition++) {
            long offset = 0;
            while (readRecords < records) {
                LogReadResult result = logManager.read(topic, partition, offset, 4 * 1024 * 1024);
                if (result == null || result.records().length == 0) {
                    break;
                }
                bytes += result.records().length;
                readRecords += Math.max(1, result.records().length / Math.max(recordBytes, 1));
                offset = result.nextFetchOffset();
            }
        }
        long elapsed = System.nanoTime() - started;
        return BenchmarkResult.of("read", readRecords, bytes, elapsed);
    }

    private static byte[] payload(int recordBytes) {
        byte[] payload = new byte[recordBytes];
        byte[] seed = "stellflow-benchmark".getBytes(StandardCharsets.UTF_8);
        for (int index = 0; index < payload.length; index++) {
            payload[index] = seed[index % seed.length];
        }
        return payload;
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> values = new HashMap<>();
        for (String arg : args) {
            if (!arg.startsWith("--")) {
                continue;
            }
            String[] parts = arg.substring(2).split("=", 2);
            values.put(parts[0], parts.length == 2 ? parts[1] : "true");
        }
        return values;
    }

    private static int parsePositiveInt(Map<String, String> options, String key, int defaultValue) {
        int value = Integer.parseInt(options.getOrDefault(key, Integer.toString(defaultValue)));
        if (value <= 0) {
            throw new IllegalArgumentException(key + " must be positive");
        }
        return value;
    }

    private static void print(BenchmarkResult result) {
        System.out.printf(
                "%s records=%d bytes=%d elapsedMs=%d recordsPerSecond=%.2f megabytesPerSecond=%.2f%n",
                result.name(),
                result.records(),
                result.bytes(),
                result.elapsedMs(),
                result.recordsPerSecond(),
                result.megabytesPerSecond());
    }
}
