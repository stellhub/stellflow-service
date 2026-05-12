package io.github.stellhub.stellflow.controller.control;

import io.github.stellhub.stellflow.controlplane.PartitionControlApplyResult;
import io.github.stellhub.stellflow.controlplane.PartitionControlCommandMessage;
import io.github.stellhub.stellflow.controlplane.ReplicaAssignment;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    private final Map<String, ControllerTopicMetadata> topics = new ConcurrentHashMap<>();
    private final Map<String, ControllerPartitionMetadata> partitions = new ConcurrentHashMap<>();
    private final Map<Integer, Map<String, PartitionControlCommandMessage>> pendingDeleteCommandsByBroker =
            new ConcurrentHashMap<>();

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
                        brokerId,
                        advertisedEndpoint,
                        advertisedHost,
                        advertisedPort,
                        registeredAtMs,
                        false));
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
        ControllerPartitionMetadata current = partitions.get(key(topic, partition));
        queueDeleteCommandsForPartition(current);
        partitions.remove(key(topic, partition));
        recomputeSnapshots();
    }

    /**
     * 应用 topic 创建结果。
     */
    public synchronized void createTopic(String topic, int partitionCount, long createdAtMs) {
        topics.put(topic, new ControllerTopicMetadata(topic, partitionCount, createdAtMs));
        recomputeSnapshots();
    }

    /**
     * 删除 topic 及其所有分区。
     */
    public synchronized void deleteTopic(String topic) {
        topics.remove(topic);
        for (ControllerPartitionMetadata metadata : partitions.values()) {
            if (metadata.topic().equals(topic)) {
                queueDeleteCommandsForPartition(metadata);
            }
        }
        partitions.entrySet().removeIf(entry -> entry.getValue().topic().equals(topic));
        recomputeSnapshots();
    }

    /**
     * 扩大 topic 分区数。
     */
    public synchronized void expandTopicPartitions(String topic, int partitionCount) {
        ControllerTopicMetadata current = topics.get(topic);
        long createdAtMs = current != null ? current.createdAtMs() : System.currentTimeMillis();
        topics.put(topic, new ControllerTopicMetadata(topic, partitionCount, createdAtMs));
        recomputeSnapshots();
    }

    /**
     * 应用分区拓扑变更。
     */
    public synchronized void updatePartitionTopology(String topic, int partition, List<Integer> replicaNodes) {
        ControllerPartitionMetadata current = partitions.get(key(topic, partition));
        List<Integer> sanitizedReplicas =
                replicaNodes == null || replicaNodes.isEmpty()
                        ? current != null && current.replicaNodes() != null && !current.replicaNodes().isEmpty()
                                ? current.replicaNodes()
                                : List.of()
                        : List.copyOf(replicaNodes);
        int leaderId =
                current != null
                        ? current.leaderId()
                        : sanitizedReplicas.isEmpty() ? -1 : sanitizedReplicas.get(0);
        int leaderEpoch = current != null ? current.leaderEpoch() : 0;
        List<Integer> isrNodes =
                current != null && current.isrNodes() != null && !current.isrNodes().isEmpty()
                        ? current.isrNodes()
                        : sanitizedReplicas;
        ControllerPartitionMetadata merged =
                sanitize(
                        new ControllerPartitionMetadata(
                                topic,
                                partition,
                                leaderId,
                                leaderEpoch,
                                sanitizedReplicas,
                                isrNodes,
                                current == null ? null : current.truncateToLeaderEpoch(),
                                current == null ? null : current.truncateToOffset()));
        if (current != null) {
            for (Integer brokerId : current.replicaNodes()) {
                if (!merged.replicaNodes().contains(brokerId)) {
                    queueDeleteCommand(
                            brokerId,
                            topic,
                            partition,
                            Math.max(current.leaderEpoch(), merged.leaderEpoch()));
                }
            }
        }
        partitions.put(key(topic, partition), merged);
        recomputeSnapshots();
    }

    /**
     * 应用 leader 与 ISR 变更。
     */
    public synchronized void updatePartitionLeaderAndIsr(
            String topic,
            int partition,
            int leaderId,
            int leaderEpoch,
            List<Integer> isrNodes,
            Integer truncateToLeaderEpoch,
            Long truncateToOffset) {
        ControllerPartitionMetadata current = partitions.get(key(topic, partition));
        List<Integer> replicaNodes =
                current != null && current.replicaNodes() != null && !current.replicaNodes().isEmpty()
                        ? current.replicaNodes()
                        : List.of(leaderId);
        ControllerPartitionMetadata merged =
                sanitize(
                        new ControllerPartitionMetadata(
                                topic,
                                partition,
                                leaderId,
                                leaderEpoch,
                                replicaNodes,
                                isrNodes,
                                truncateToLeaderEpoch,
                                truncateToOffset));
        partitions.put(key(topic, partition), merged);
        recomputeSnapshots();
    }

    /**
     * 将 broker 标记为 fenced。
     */
    public synchronized void fenceBroker(int brokerId) {
        BrokerRegistrationMetadata current = brokers.get(brokerId);
        if (current == null || current.fenced()) {
            return;
        }
        brokers.put(
                brokerId,
                new BrokerRegistrationMetadata(
                        current.brokerId(),
                        current.advertisedEndpoint(),
                        current.advertisedHost(),
                        current.advertisedPort(),
                        current.registeredAtMs(),
                        true));
        recomputeSnapshots();
    }

    /**
     * 将 broker 标记为 unfenced。
     */
    public synchronized void unfenceBroker(int brokerId) {
        BrokerRegistrationMetadata current = brokers.get(brokerId);
        if (current == null || !current.fenced()) {
            return;
        }
        brokers.put(
                brokerId,
                new BrokerRegistrationMetadata(
                        current.brokerId(),
                        current.advertisedEndpoint(),
                        current.advertisedHost(),
                        current.advertisedPort(),
                        current.registeredAtMs(),
                        false));
        recomputeSnapshots();
    }

    /**
     * 收缩分区 ISR。
     */
    public synchronized void shrinkPartitionIsr(String topic, int partition, int brokerId, int leaderEpoch) {
        ControllerPartitionMetadata current = partitions.get(key(topic, partition));
        if (current == null) {
            return;
        }
        List<Integer> nextIsr =
                current.isrNodes().stream()
                        .filter(id -> id != brokerId)
                        .collect(Collectors.toCollection(ArrayList::new));
        if (nextIsr.isEmpty()) {
            nextIsr.add(current.leaderId());
        }
        updatePartitionLeaderAndIsr(
                topic,
                partition,
                current.leaderId(),
                Math.max(current.leaderEpoch(), leaderEpoch),
                List.copyOf(nextIsr),
                current.truncateToLeaderEpoch(),
                current.truncateToOffset());
    }

    /**
     * 扩大分区 ISR。
     */
    public synchronized void expandPartitionIsr(String topic, int partition, int brokerId, int leaderEpoch) {
        ControllerPartitionMetadata current = partitions.get(key(topic, partition));
        if (current == null) {
            return;
        }
        List<Integer> nextIsr = new ArrayList<>(current.isrNodes());
        if (!nextIsr.contains(brokerId)) {
            nextIsr.add(brokerId);
        }
        updatePartitionLeaderAndIsr(
                topic,
                partition,
                current.leaderId(),
                Math.max(current.leaderEpoch(), leaderEpoch),
                List.copyOf(nextIsr),
                current.truncateToLeaderEpoch(),
                current.truncateToOffset());
    }

    /**
     * 记录 broker 对 partition control 的应用结果。
     */
    public synchronized void recordPartitionControlResults(
            int brokerId, List<PartitionControlApplyResult> results) {
        partitionControlResultRegistry.record(brokerId, results);
        Map<String, PartitionControlCommandMessage> pendingDeletes =
                pendingDeleteCommandsByBroker.get(brokerId);
        if (pendingDeletes != null) {
            for (PartitionControlApplyResult result : results) {
                if (result.getSuccess() && result.getDeletePartition()) {
                    pendingDeletes.remove(key(result.getTopic(), result.getPartition()));
                }
            }
            if (pendingDeletes.isEmpty()) {
                pendingDeleteCommandsByBroker.remove(brokerId);
            }
        }
        recomputeSnapshots();
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

    /**
     * 返回单个 broker 元数据。
     */
    public synchronized Optional<BrokerRegistrationMetadata> broker(int brokerId) {
        return Optional.ofNullable(brokers.get(brokerId));
    }

    /**
     * 返回 topic 元数据快照。
     */
    public synchronized List<ControllerTopicMetadata> topics() {
        return List.copyOf(topics.values());
    }

    /**
     * 返回单个 topic 元数据。
     */
    public synchronized Optional<ControllerTopicMetadata> topic(String topic) {
        return Optional.ofNullable(topics.get(topic));
    }

    /**
     * 返回单个分区元数据。
     */
    public synchronized Optional<ControllerPartitionMetadata> partition(String topic, int partition) {
        return Optional.ofNullable(partitions.get(key(topic, partition)));
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
                BrokerRegistrationMetadata replica = brokers.get(brokerId);
                if (replica == null || replica.fenced()) {
                    continue;
                }
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
            if (leader == null || leader.fenced()) {
                continue;
            }
            for (Integer brokerId : metadata.replicaNodes()) {
                if (brokerId == metadata.leaderId()) {
                    continue;
                }
                BrokerRegistrationMetadata follower = brokers.get(brokerId);
                if (follower == null || follower.fenced()) {
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

        for (Map.Entry<Integer, Map<String, PartitionControlCommandMessage>> entry :
                pendingDeleteCommandsByBroker.entrySet()) {
            allBrokerIds.add(entry.getKey());
            commandsByBroker
                    .computeIfAbsent(entry.getKey(), ignored -> new ArrayList<>())
                    .addAll(entry.getValue().values());
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

    private void queueDeleteCommandsForPartition(ControllerPartitionMetadata metadata) {
        if (metadata == null) {
            return;
        }
        for (Integer brokerId : metadata.replicaNodes()) {
            queueDeleteCommand(brokerId, metadata.topic(), metadata.partition(), metadata.leaderEpoch());
        }
    }

    private void queueDeleteCommand(int brokerId, String topic, int partition, int leaderEpoch) {
        pendingDeleteCommandsByBroker
                .computeIfAbsent(brokerId, ignored -> new ConcurrentHashMap<>())
                .put(
                        key(topic, partition),
                        ControllerPartitionControlRegistry.deletePartitionCommand(
                                topic, partition, leaderEpoch));
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
