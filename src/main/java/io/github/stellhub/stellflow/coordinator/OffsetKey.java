package io.github.stellhub.stellflow.coordinator;

/**
 * 消费位点键。
 */
public record OffsetKey(String groupId, String topic, int partition) {}
