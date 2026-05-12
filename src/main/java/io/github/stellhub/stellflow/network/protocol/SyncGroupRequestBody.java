package io.github.stellhub.stellflow.network.protocol;

/**
 * SyncGroup 请求体。
 */
public record SyncGroupRequestBody(String groupId, int generationId, String memberId)
        implements RequestBody {}
