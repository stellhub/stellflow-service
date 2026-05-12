package io.github.stellhub.stellflow.coordinator;

import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Consumer Group 协调器最小实现。
 */
public class ConsumerGroupCoordinator {

    private final OffsetStore offsetStore;
    private final Map<String, GroupRuntime> groups = new HashMap<>();

    public ConsumerGroupCoordinator(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    /**
     * 加入消费组。
     */
    public synchronized JoinResult joinGroup(
            String groupId, String memberId, String clientId, String clientHost) {
        GroupRuntime group = groups.computeIfAbsent(groupId, ignored -> new GroupRuntime());
        String actualMemberId =
                memberId == null || memberId.isBlank()
                        ? "stellflow-member-" + UUID.randomUUID()
                        : memberId;
        group.generationId++;
        group.state = ConsumerGroupState.STABLE;
        group.members.put(
                actualMemberId,
                new ConsumerGroupMember(actualMemberId, clientId, clientHost, System.currentTimeMillis()));
        if (group.leaderId == null || !group.members.containsKey(group.leaderId)) {
            group.leaderId = actualMemberId;
        }
        return new JoinResult(ErrorCode.NONE, group.generationId, actualMemberId, group.leaderId);
    }

    /**
     * 同步消费组。
     */
    public synchronized ErrorCode syncGroup(String groupId, int generationId, String memberId) {
        GroupRuntime group = groups.get(groupId);
        if (group == null || !group.members.containsKey(memberId)) {
            return ErrorCode.UNKNOWN_SERVER_ERROR;
        }
        if (generationId != group.generationId) {
            return ErrorCode.FENCED_INSTANCE_ID;
        }
        group.state = ConsumerGroupState.STABLE;
        return ErrorCode.NONE;
    }

    /**
     * 处理心跳。
     */
    public synchronized ErrorCode heartbeat(String groupId, int generationId, String memberId) {
        GroupRuntime group = groups.get(groupId);
        if (group == null || !group.members.containsKey(memberId)) {
            return ErrorCode.UNKNOWN_SERVER_ERROR;
        }
        if (generationId != group.generationId) {
            return ErrorCode.FENCED_INSTANCE_ID;
        }
        ConsumerGroupMember current = group.members.get(memberId);
        group.members.put(
                memberId,
                new ConsumerGroupMember(
                        current.memberId(),
                        current.clientId(),
                        current.clientHost(),
                        System.currentTimeMillis()));
        return ErrorCode.NONE;
    }

    public OffsetStore offsetStore() {
        return offsetStore;
    }

    /**
     * 消费组加入结果。
     */
    public record JoinResult(ErrorCode errorCode, int generationId, String memberId, String leaderId) {}

    private static final class GroupRuntime {
        private final Map<String, ConsumerGroupMember> members = new HashMap<>();
        private ConsumerGroupState state = ConsumerGroupState.EMPTY;
        private int generationId;
        private String leaderId;
    }
}
