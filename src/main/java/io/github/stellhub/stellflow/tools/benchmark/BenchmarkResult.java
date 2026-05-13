package io.github.stellhub.stellflow.tools.benchmark;

/**
 * Benchmark 执行结果。
 */
public record BenchmarkResult(
        String name,
        long records,
        long bytes,
        long elapsedMs,
        double recordsPerSecond,
        double megabytesPerSecond) {

    /**
     * 构建结果对象。
     */
    public static BenchmarkResult of(String name, long records, long bytes, long elapsedNanos) {
        double elapsedSeconds = elapsedNanos / 1_000_000_000.0;
        double recordsPerSecond = elapsedSeconds == 0 ? 0 : records / elapsedSeconds;
        double megabytesPerSecond = elapsedSeconds == 0 ? 0 : bytes / 1024.0 / 1024.0 / elapsedSeconds;
        return new BenchmarkResult(
                name,
                records,
                bytes,
                elapsedNanos / 1_000_000,
                recordsPerSecond,
                megabytesPerSecond);
    }
}
