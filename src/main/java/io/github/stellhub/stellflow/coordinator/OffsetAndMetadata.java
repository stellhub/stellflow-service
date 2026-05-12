package io.github.stellhub.stellflow.coordinator;

/**
 * 消费位点与元信息。
 */
public record OffsetAndMetadata(long offset, String metadata, long commitTimestampMs) {}
