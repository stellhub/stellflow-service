package io.github.stellhub.stellflow.coordinator;

/**
 * 消费组成员。
 */
public record ConsumerGroupMember(
        String memberId, String clientId, String clientHost, long lastHeartbeatMs) {}
