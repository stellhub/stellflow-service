package io.github.stellhub.stellflow.storage.log;

/**
 * 日志读取结果。
 */
public record LogReadResult(
        long highWatermark,
        long logStartOffset,
        long lastStableOffset,
        long nextFetchOffset,
        byte[] records) {}
