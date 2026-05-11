package io.github.stellhub.stellflow.storage.log;

/**
 * topic-partition 标识。
 */
public record TopicPartition(String topic, int partition) {}
