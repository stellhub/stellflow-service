package io.github.stellhub.stellflow.controller.control;

import io.github.stellhub.stellflow.controlplane.PartitionControlApplyResult;
import io.github.stellhub.stellflow.controlplane.PartitionControlCommandMessage;
import io.github.stellhub.stellflow.controlplane.ReplicaAssignment;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Controller 元数据状态机。
 */
public class ControllerMetadataStateMachine {

    private final ControllerAssignmentRegistry assignmentRegistry;
    private final ControllerPartitionControlRegistry partitionControlRegistry;
    private final PartitionControlResultRegistry partitionControlResultRegistry;
    private final Map<Integer, BrokerRegistrationMetadata> brokers = new ConcurrentHashMap<>();
    private final Map<String, ControllerPartitionMetadata> partitions = new ConcurrentHashMap<>();

    public ControllerMetadataStateMachine(
            ControllerAssignmentRegistry assignmentRegistry,
            ControllerPartitionControlRegistry partitionControlRegistry,
            PartitionControlResultRegistry partitionControlResultRegistry) {
        this.assignmentRegistry = assignmentRegistry;
        this.partitionControlRegistry = partitionControlRegistry;
        this.partitionControlResultRegistry = partitionControlResultRegistry;
    }

    /**
     * 记录 broker 注册信息并重新驱动下游快照。
     */
    public synchronized void registerBroker(
            int brokerId, String advertisedEndpoint, String advertisedHost, int advertisedPort) {
        registerBroker(
                brokerId,
                advertisedEndpoint,
                advertisedHost,
                advertisedPort,
                System.currentTimeMillis());
    }

    /**
     * 记录 broker 注册信息并保留外部提交时间。
     */
    public synchronized void registerBroker(
            int brokerId,
            String advertisedEndpoint,
            String advertisedHost,
            int advertisedPort,
            long registeredAtMs) {
        brokers.put(
                brokerId,
                new BrokerRegistrationMetadata(
                        brokerId, advertisedEndpoint, advertisedHost, advertisedPort, registeredAtMs));
        recomputeSnapshots();
    }

    /**
     * 批量替换当前控制器维护的分区元数据。
     */
    public synchronized void replacePartitions(List<ControllerPartitionMetadata> metadataList) {
        partitions.clear();
        for (ControllerPartitionMetadata metadata : metadataList) {
            partitions.put(key(metadata.topic(), metadata.partition()), sanitize(metadata));
        }
        recomputeSnapshots();
    }

    /**
     * 插入或更新单个分区元数据。
     */
    public synchronized void upsertPartition(ControllerPartitionMetadata metadata) {
        ControllerPartitionMetadata sanitized = sanitize(metadata);
        partitions.put(key(sanitized.topic(), sanitized.partition()), sanitized);
        recomputeSnapshots();
    }

    /**
     * 删除单个分区元数据。
     */
    public synchronized void removePartition(String topic, int partition) {
        partitions.remove(key(topic, partition));
        recomputeSnapshots();
    }

    /**
     * 记录 broker 对 partition control 的应用结果。
     */
    public synchronized void recordPartitionControlResults(
            int brokerId, List<PartitionControlApplyResult> results) {
        partitionControlResultRegistry.record(brokerId, results);
    }

    /**
     * 返回当前分区元数据快照。
     */
    public synchronized List<ControllerPartitionMetadata> partitions() {
        return List.copyOf(partitions.values());
    }

    /**
     * 返回 broker 注册快照。
     */
    public synchronized List<BrokerRegistrationMetadata> brokers() {
        return List.copyOf(brokers.values());
    }

    private ControllerPartitionMetadata sanitize(ControllerPartitionMetadata metadata) {
        List<Integer> replicaNodes =
                metadata.replicaNodes() == null || metadata.replicaNodes().isEmpty()
                        ? List.of(metadata.leaderId())
                        : metadata.replicaNodes();
        List<Integer> isrNodes =
                metadata.isrNodes() == null || metadata.isrNodes().isEmpty()
                        ? List.of(metadata.leaderId())
                        : metadata.isrNodes();
        if (!replicaNodes.contains(metadata.leaderId())) {
            replicaNodes = new ArrayList<>(replicaNodes);
            replicaNodes.add(metadata.leaderId());
        }
        if (!isrNodes.contains(metadata.leaderId())) {
            isrNodes = new ArrayList<>(isrNodes);
            isrNodes.add(metadata.leaderId());
        }
        return new ControllerPartitionMetadata(
                metadata.topic(),
                metadata.partition(),
                metadata.leaderId(),
                metadata.leaderEpoch(),
                List.copyOf(replicaNodes),
                List.copyOf(isrNodes),
                metadata.truncateToLeaderEpoch(),
                metadata.truncateToOffset());
    }

    private void recomputeSnapshots() {
        Map<Integer, List<ReplicaAssignment>> assignmentsByBroker = new HashMap<>();
        Map<Integer, List<PartitionControlCommandMessage>> commandsByBroker = new HashMap<>();
        Set<Integer> allBrokerIds = new TreeSet<>(brokers.keySet());

        for (ControllerPartitionMetadata metadata : partitions.values()) {
            for (Integer brokerId : metadata.replicaNodes()) {
                allBrokerIds.add(brokerId);
                commandsByBroker
                        .computeIfAbsent(brokerId, ignored -> new ArrayList<>())
                        .add(
                                ControllerPartitionControlRegistry.command(
                                        metadata.topic(),
                                        metadata.partition(),
                                        metadata.leaderId(),
                                        metadata.leaderEpoch(),
                                        metadata.replicaNodes(),
                                        metadata.isrNodes(),
                                        metadata.truncateToLeaderEpoch(),
                                        metadata.truncateToOffset()));
            }

            BrokerRegistrationMetadata leader = brokers.get(metadata.leaderId());
            if (leader == null) {
                continue;
            }
            for (Integer brokerId : metadata.replicaNodes()) {
                if (brokerId == metadata.leaderId()) {
                    continue;
                }
                assignmentsByBroker
                        .computeIfAbsent(brokerId, ignored -> new ArrayList<>())
                        .add(
                                ControllerAssignmentRegistry.assignment(
                                        metadata.topic(),
                                        metadata.partition(),
                                        leader.advertisedHost(),
                                        leader.advertisedPort(),
                                        metadata.leaderId()));
            }
        }

        for (Integer brokerId : allBrokerIds) {
            assignmentRegistry.replaceAssignments(
                    brokerId, List.copyOf(assignmentsByBroker.getOrDefault(brokerId, List.of())));
            partitionControlRegistry.replaceCommands(
                    brokerId, List.copyOf(commandsByBroker.getOrDefault(brokerId, List.of())));
        }
    }

    private String key(String topic, int partition) {
        return topic + ":" + partition;
    }

    /**
     * 构建单分区元数据。
     */
    public static ControllerPartitionMetadata partition(
            String topic,
            int partition,
            int leaderId,
            int leaderEpoch,
            List<Integer> replicaNodes,
            List<Integer> isrNodes,
            Integer truncateToLeaderEpoch,
            Long truncateToOffset) {
        return ControllerPartitionMetadata.builder()
                .topic(topic)
                .partition(partition)
                .leaderId(leaderId)
                .leaderEpoch(leaderEpoch)
                .replicaNodes(
                        replicaNodes == null ? List.of() : replicaNodes.stream().collect(Collectors.toList()))
                .isrNodes(isrNodes == null ? List.of() : isrNodes.stream().collect(Collectors.toList()))
                .truncateToLeaderEpoch(truncateToLeaderEpoch)
                .truncateToOffset(truncateToOffset)
                .build();
    }
}
