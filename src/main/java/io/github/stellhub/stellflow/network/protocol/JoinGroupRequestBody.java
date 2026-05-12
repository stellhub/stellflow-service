package io.github.stellhub.stellflow.network.protocol;

/**
 * JoinGroup 请求体。
 */
public record JoinGroupRequestBody(String groupId, String memberId, int sessionTimeoutMs)
        implements RequestBody {}
