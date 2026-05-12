package io.github.stellhub.stellflow.controller.control;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Controller 元数据规划器。
 */
public class ControllerMetadataPlanner {

    /**
     * 规划 topic 创建。
     */
    public List<ControllerPartitionMetadata> planTopicCreation(
            String topic,
            int partitionCount,
            int replicationFactor,
            List<BrokerRegistrationMetadata> brokers,
            int partitionRotationSeed) {
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be positive");
        }
        List<BrokerRegistrationMetadata> available = availableBrokers(brokers);
        if (available.isEmpty()) {
            throw new IllegalArgumentException("No available brokers for topic creation");
        }
        int effectiveReplicationFactor = Math.min(replicationFactor, available.size());
        List<ControllerPartitionMetadata> partitions = new ArrayList<>(partitionCount);
        for (int partition = 0; partition < partitionCount; partition++) {
            List<Integer> replicas =
                    chooseReplicas(
                            partitionRotationSeed + partition,
                            effectiveReplicationFactor,
                            available);
            int leaderId = replicas.get(0);
            partitions.add(
                    ControllerMetadataStateMachine.partition(
                            topic,
                            partition,
                            leaderId,
                            1,
                            replicas,
                            List.copyOf(replicas),
                            null,
                            null));
        }
        return List.copyOf(partitions);
    }

    /**
     * 规划 topic 分区扩容时新增分区。
     */
    public List<ControllerPartitionMetadata> planTopicExpansion(
            String topic,
            int currentPartitionCount,
            int targetPartitionCount,
            int replicationFactor,
            List<BrokerRegistrationMetadata> brokers) {
        if (targetPartitionCount <= currentPartitionCount) {
            return List.of();
        }
        List<BrokerRegistrationMetadata> available = availableBrokers(brokers);
        if (available.isEmpty()) {
            throw new IllegalArgumentException("No available brokers for topic expansion");
        }
        int effectiveReplicationFactor = Math.min(replicationFactor, available.size());
        List<ControllerPartitionMetadata> partitions = new ArrayList<>();
        for (int partition = currentPartitionCount; partition < targetPartitionCount; partition++) {
            List<Integer> replicas = chooseReplicas(partition, effectiveReplicationFactor, available);
            int leaderId = replicas.get(0);
            partitions.add(
                    ControllerMetadataStateMachine.partition(
                            topic,
                            partition,
                            leaderId,
                            1,
                            replicas,
                            List.copyOf(replicas),
                            null,
                            null));
        }
        return List.copyOf(partitions);
    }

    /**
     * 基于当前拓扑和 broker 可用性重算 leader 与 ISR。
     */
    public ControllerPartitionMetadata reconcilePartition(
            ControllerPartitionMetadata current, List<BrokerRegistrationMetadata> brokers) {
        List<BrokerRegistrationMetadata> available = availableBrokers(brokers);
        List<Integer> availableReplicaIds =
                current.replicaNodes().stream()
                        .filter(replicaId -> available.stream().anyMatch(broker -> broker.brokerId() == replicaId))
                        .toList();
        List<Integer> currentIsr =
                current.isrNodes().stream()
                        .filter(availableReplicaIds::contains)
                        .toList();
        int nextLeaderId =
                availableReplicaIds.contains(current.leaderId())
                        ? current.leaderId()
                        : !currentIsr.isEmpty()
                                ? currentIsr.get(0)
                                : !availableReplicaIds.isEmpty()
                                        ? availableReplicaIds.get(0)
                                        : current.leaderId();
        List<Integer> nextIsr =
                currentIsr.isEmpty()
                        ? List.of(nextLeaderId)
                        : currentIsr.contains(nextLeaderId)
                                ? List.copyOf(currentIsr)
                                : prependLeader(nextLeaderId, currentIsr);
        return ControllerMetadataStateMachine.partition(
                current.topic(),
                current.partition(),
                nextLeaderId,
                Math.max(current.leaderEpoch(), 1),
                current.replicaNodes(),
                nextIsr,
                current.truncateToLeaderEpoch(),
                current.truncateToOffset());
    }

    private static List<Integer> prependLeader(int leaderId, List<Integer> currentIsr) {
        List<Integer> nextIsr = new ArrayList<>(currentIsr.size() + 1);
        nextIsr.add(leaderId);
        for (Integer brokerId : currentIsr) {
            if (brokerId != leaderId) {
                nextIsr.add(brokerId);
            }
        }
        return List.copyOf(nextIsr);
    }

    private static List<BrokerRegistrationMetadata> availableBrokers(
            List<BrokerRegistrationMetadata> brokers) {
        return brokers.stream()
                .filter(Objects::nonNull)
                .filter(broker -> !broker.fenced())
                .sorted(Comparator.comparingInt(BrokerRegistrationMetadata::brokerId))
                .collect(Collectors.toUnmodifiableList());
    }

    private static List<Integer> chooseReplicas(
            int partition, int replicationFactor, List<BrokerRegistrationMetadata> available) {
        List<Integer> replicas = new ArrayList<>(replicationFactor);
        int start = partition % available.size();
        for (int offset = 0; offset < replicationFactor; offset++) {
            replicas.add(available.get((start + offset) % available.size()).brokerId());
        }
        return List.copyOf(replicas);
    }
}
