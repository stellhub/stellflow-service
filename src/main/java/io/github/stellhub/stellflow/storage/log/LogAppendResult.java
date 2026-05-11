package io.github.stellhub.stellflow.storage.log;

/**
 * 日志追加结果。
 */
public record LogAppendResult(
        long baseOffset, long logEndOffset, long highWatermark, int leaderEpoch) {}
