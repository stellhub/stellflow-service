package io.github.stellhub.stellflow.server.runtime;

import io.github.stellhub.stellflow.network.protocol.ErrorCode;

/**
 * Topic 管理结果。
 */
public record TopicControlResult(String topic, int partition, ErrorCode errorCode, int leaderEpoch) {}
