package io.github.stellhub.stellflow.producer;

/**
 * Producer 状态。
 */
public record ProducerState(long producerId, short producerEpoch, long lastSeenMs) {}
