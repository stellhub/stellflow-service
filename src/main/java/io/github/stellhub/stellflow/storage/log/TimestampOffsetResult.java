package io.github.stellhub.stellflow.storage.log;

/**
 * 按时间查询 offset 的结果。
 */
public record TimestampOffsetResult(long offset, long timestamp) {}
