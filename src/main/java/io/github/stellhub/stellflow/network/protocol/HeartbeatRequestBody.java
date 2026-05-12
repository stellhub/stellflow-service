package io.github.stellhub.stellflow.network.protocol;

/**
 * Heartbeat 请求体。
 */
public record HeartbeatRequestBody(String groupId, int generationId, String memberId)
        implements RequestBody {}
