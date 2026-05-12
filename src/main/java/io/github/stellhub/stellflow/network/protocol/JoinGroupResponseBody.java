package io.github.stellhub.stellflow.network.protocol;

/**
 * JoinGroup 响应体。
 */
public record JoinGroupResponseBody(
        ErrorCode errorCode, int generationId, String memberId, String leaderId)
        implements ResponseBody {}
