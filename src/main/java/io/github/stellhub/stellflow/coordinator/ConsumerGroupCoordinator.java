package io.github.stellhub.stellflow.coordinator;

import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.storage.log.TopicPartition;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Consumer Group 协调器最小实现。
 */
public class ConsumerGroupCoordinator {

    private final OffsetStore offsetStore;
    private final RangePartitionAssignor assignor;
    private final Map<String, GroupRuntime> groups = new HashMap<>();

    public ConsumerGroupCoordinator(OffsetStore offsetStore) {
        this(offsetStore, new RangePartitionAssignor());
    }

    public ConsumerGroupCoordinator(OffsetStore offsetStore, RangePartitionAssignor assignor) {
        this.offsetStore = offsetStore;
        this.assignor = assignor;
    }

    /**
     * 加入消费组。
     */
    public synchronized JoinResult joinGroup(
            String groupId,
            String memberId,
            String clientId,
            String clientHost,
            int sessionTimeoutMs) {
        GroupRuntime group = groups.computeIfAbsent(groupId, ignored -> new GroupRuntime());
        evictExpiredMembers(group, System.currentTimeMillis());
        String actualMemberId =
                memberId == null || memberId.isBlank()
                        ? "stellflow-member-" + UUID.randomUUID()
                        : memberId;
        group.state = ConsumerGroupState.PREPARING_REBALANCE;
        group.members.put(
                actualMemberId,
                new ConsumerGroupMember(
                        actualMemberId,
                        clientId,
                        clientHost,
                        System.currentTimeMillis(),
                        Math.max(sessionTimeoutMs, 1)));
        group.generationId++;
        if (group.leaderId == null || !group.members.containsKey(group.leaderId)) {
            group.leaderId = actualMemberId;
        }
        recomputeAssignments(group);
        group.state = group.members.isEmpty() ? ConsumerGroupState.EMPTY : ConsumerGroupState.STABLE;
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
        evictExpiredMembers(group, System.currentTimeMillis());
        if (!group.members.containsKey(memberId)) {
            return ErrorCode.FENCED_INSTANCE_ID;
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
        evictExpiredMembers(group, System.currentTimeMillis());
        if (!group.members.containsKey(memberId)) {
            return ErrorCode.FENCED_INSTANCE_ID;
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
                        System.currentTimeMillis(),
                        current.sessionTimeoutMs()));
        return ErrorCode.NONE;
    }

    /**
     * 设置消费组可分配分区并触发再均衡。
     */
    public synchronized void updateAssignablePartitions(
            String groupId, List<TopicPartition> partitions) {
        GroupRuntime group = groups.computeIfAbsent(groupId, ignored -> new GroupRuntime());
        group.assignablePartitions = List.copyOf(partitions);
        group.state = ConsumerGroupState.PREPARING_REBALANCE;
        group.generationId++;
        recomputeAssignments(group);
        group.state = group.members.isEmpty() ? ConsumerGroupState.EMPTY : ConsumerGroupState.STABLE;
    }

    /**
     * 驱逐所有超时成员。
     */
    public synchronized int evictExpiredMembers(long nowMs) {
        int removed = 0;
        for (GroupRuntime group : groups.values()) {
            removed += evictExpiredMembers(group, nowMs);
        }
        return removed;
    }

    /**
     * 返回成员当前分配。
     */
    public synchronized List<TopicPartition> assignment(String groupId, String memberId) {
        GroupRuntime group = groups.get(groupId);
        if (group == null) {
            return List.of();
        }
        return group.assignments.getOrDefault(memberId, List.of());
    }

    public OffsetStore offsetStore() {
        return offsetStore;
    }

    private int evictExpiredMembers(GroupRuntime group, long nowMs) {
        List<String> expiredMembers = new ArrayList<>();
        for (ConsumerGroupMember member : group.members.values()) {
            if (nowMs - member.lastHeartbeatMs() > member.sessionTimeoutMs()) {
                expiredMembers.add(member.memberId());
            }
        }
        if (expiredMembers.isEmpty()) {
            return 0;
        }
        for (String memberId : expiredMembers) {
            group.members.remove(memberId);
            group.assignments.remove(memberId);
        }
        if (group.leaderId != null && !group.members.containsKey(group.leaderId)) {
            group.leaderId = group.members.keySet().stream().sorted().findFirst().orElse(null);
        }
        group.generationId++;
        group.state = group.members.isEmpty() ? ConsumerGroupState.EMPTY : ConsumerGroupState.PREPARING_REBALANCE;
        recomputeAssignments(group);
        if (!group.members.isEmpty()) {
            group.state = ConsumerGroupState.STABLE;
        }
        return expiredMembers.size();
    }

    private void recomputeAssignments(GroupRuntime group) {
        List<String> memberIds = group.members.keySet().stream().sorted().toList();
        group.assignments.clear();
        for (PartitionAssignment assignment : assignor.assign(memberIds, group.assignablePartitions)) {
            group.assignments.put(assignment.memberId(), assignment.partitions());
        }
    }

    /**
     * 消费组加入结果。
     */
    public record JoinResult(ErrorCode errorCode, int generationId, String memberId, String leaderId) {}

    private static final class GroupRuntime {
        private final Map<String, ConsumerGroupMember> members = new HashMap<>();
        private final Map<String, List<TopicPartition>> assignments = new HashMap<>();
        private List<TopicPartition> assignablePartitions = List.of();
        private ConsumerGroupState state = ConsumerGroupState.EMPTY;
        private int generationId;
        private String leaderId;
    }
}
