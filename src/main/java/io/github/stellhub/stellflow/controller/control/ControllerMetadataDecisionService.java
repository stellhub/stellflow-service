package io.github.stellhub.stellflow.controller.control;

import io.github.stellhub.stellflow.controller.quorum.ControllerMetadataRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Controller 元数据决策服务。
 */
public class ControllerMetadataDecisionService {

    private final ControllerMetadataCommandService metadataCommandService;
    private final ControllerMetadataStateMachine metadataStateMachine;
    private final ControllerMetadataPlanner planner;
    private final boolean uncleanLeaderElectionEnabled;

    public ControllerMetadataDecisionService(
            ControllerMetadataCommandService metadataCommandService,
            ControllerMetadataStateMachine metadataStateMachine) {
        this(metadataCommandService, metadataStateMachine, false);
    }

    public ControllerMetadataDecisionService(
            ControllerMetadataCommandService metadataCommandService,
            ControllerMetadataStateMachine metadataStateMachine,
            boolean uncleanLeaderElectionEnabled) {
        this.metadataCommandService = metadataCommandService;
        this.metadataStateMachine = metadataStateMachine;
        this.planner = new ControllerMetadataPlanner();
        this.uncleanLeaderElectionEnabled = uncleanLeaderElectionEnabled;
    }

    /**
     * 提交 broker 注册并纳入后续元数据规划。
     */
    public void registerBroker(
            int brokerId,
            String advertisedEndpoint,
            String advertisedHost,
            int advertisedPort,
            long registeredAtMs) {
        metadataCommandService.registerBroker(
                brokerId,
                advertisedEndpoint,
                advertisedHost,
                advertisedPort,
                registeredAtMs);
    }

    /**
     * 使用既定分区元数据创建 topic，并生成对应的拓扑与 leader/ISR 元数据记录。
     */
    public void createTopicWithPartitions(String topic, List<ControllerPartitionMetadata> partitions) {
        if (partitions == null || partitions.isEmpty()) {
            throw new IllegalArgumentException("partitions must not be empty");
        }
        List<ControllerMetadataRecord> records = new ArrayList<>();
        records.add(ControllerMetadataRecord.createTopic(topic, partitions.size(), System.currentTimeMillis()));
        for (ControllerPartitionMetadata partition : partitions) {
            validateTopic(topic, partition.topic());
            records.add(
                    ControllerMetadataRecord.updatePartitionTopology(
                            topic, partition.partition(), partition.replicaNodes()));
            records.add(
                    ControllerMetadataRecord.updatePartitionLeaderAndIsr(
                            topic,
                            partition.partition(),
                            partition.leaderId(),
                            partition.leaderEpoch(),
                            partition.isrNodes(),
                            partition.truncateToLeaderEpoch(),
                            partition.truncateToOffset()));
        }
        metadataCommandService.submitAll(records);
    }

    /**
     * 基于当前 broker 集群规划并创建 topic。
     */
    public void createTopic(String topic, int partitionCount, int replicationFactor) {
        createTopicWithPartitions(
                topic,
                planner.planTopicCreation(
                        topic,
                        partitionCount,
                        replicationFactor,
                        metadataStateMachine.brokers(),
                        metadataStateMachine.partitions().size()));
    }

    /**
     * 删除 topic。
     */
    public void deleteTopic(String topic) {
        metadataCommandService.deleteTopic(topic);
    }

    /**
     * 扩容 topic 分区数。
     */
    public void expandTopicPartitions(String topic, int targetPartitionCount, int replicationFactor) {
        ControllerTopicMetadata currentTopic =
                metadataStateMachine
                        .topic(topic)
                        .orElseThrow(() -> new IllegalArgumentException("Topic not found: " + topic));
        List<ControllerPartitionMetadata> newPartitions =
                planner.planTopicExpansion(
                        topic,
                        currentTopic.partitionCount(),
                        targetPartitionCount,
                        replicationFactor,
                        metadataStateMachine.brokers());
        if (newPartitions.isEmpty()) {
            return;
        }
        List<ControllerMetadataRecord> records = new ArrayList<>();
        records.add(ControllerMetadataRecord.expandTopicPartitions(topic, targetPartitionCount));
        for (ControllerPartitionMetadata partition : newPartitions) {
            records.add(
                    ControllerMetadataRecord.updatePartitionTopology(
                            topic, partition.partition(), partition.replicaNodes()));
            records.add(
                    ControllerMetadataRecord.updatePartitionLeaderAndIsr(
                            topic,
                            partition.partition(),
                            partition.leaderId(),
                            partition.leaderEpoch(),
                            partition.isrNodes(),
                            partition.truncateToLeaderEpoch(),
                            partition.truncateToOffset()));
        }
        metadataCommandService.submitAll(records);
    }

    /**
     * broker fenced，并根据当前策略重算受影响分区的 leader/ISR。
     */
    public void fenceBroker(int brokerId) {
        metadataCommandService.fenceBroker(brokerId);
        reconcilePartitionsForBroker(brokerId);
    }

    /**
     * broker unfenced，并根据当前策略重算受影响分区的 leader/ISR。
     */
    public void unfenceBroker(int brokerId) {
        metadataCommandService.unfenceBroker(brokerId);
        reconcilePartitionsForBroker(brokerId);
    }

    /**
     * 变更分区副本拓扑。
     */
    public void changePartitionTopology(String topic, int partition, List<Integer> replicaNodes) {
        metadataCommandService.updatePartitionTopology(topic, partition, replicaNodes);
        reconcilePartition(topic, partition);
    }

    /**
     * 变更分区 leader 与 ISR。
     */
    public void changeLeaderAndIsr(
            String topic,
            int partition,
            int leaderId,
            int leaderEpoch,
            List<Integer> isrNodes,
            Integer truncateToLeaderEpoch,
            Long truncateToOffset) {
        metadataCommandService.updatePartitionLeaderAndIsr(
                topic,
                partition,
                leaderId,
                leaderEpoch,
                isrNodes,
                truncateToLeaderEpoch,
                truncateToOffset);
    }

    /**
     * 收缩 ISR。
     */
    public void shrinkIsr(String topic, int partition, int brokerId) {
        int nextEpoch =
                metadataStateMachine
                        .partition(topic, partition)
                        .map(ControllerPartitionMetadata::leaderEpoch)
                        .orElse(0);
        metadataCommandService.shrinkPartitionIsr(topic, partition, brokerId, nextEpoch);
        reconcilePartition(topic, partition);
    }

    /**
     * 扩大 ISR。
     */
    public void expandIsr(String topic, int partition, int brokerId) {
        int nextEpoch =
                metadataStateMachine
                        .partition(topic, partition)
                        .map(ControllerPartitionMetadata::leaderEpoch)
                        .orElse(0);
        metadataCommandService.expandPartitionIsr(topic, partition, brokerId, nextEpoch);
        reconcilePartition(topic, partition);
    }

    /**
     * 对单分区执行策略重算。
     */
    public void reconcilePartition(String topic, int partition) {
        Optional<ControllerPartitionMetadata> current = metadataStateMachine.partition(topic, partition);
        if (current.isEmpty()) {
            return;
        }
        ControllerPartitionMetadata next =
                planner.reconcilePartition(
                        current.get(), metadataStateMachine.brokers(), uncleanLeaderElectionEnabled);
        if (sameLeaderAndIsr(current.get(), next)) {
            return;
        }
        changeLeaderAndIsr(
                topic,
                partition,
                next.leaderId(),
                Math.max(current.get().leaderEpoch() + 1, next.leaderEpoch()),
                next.isrNodes(),
                next.truncateToLeaderEpoch(),
                next.truncateToOffset());
    }

    /**
     * 对受某个 broker 影响的分区执行策略重算。
     */
    public void reconcilePartitionsForBroker(int brokerId) {
        for (ControllerPartitionMetadata partition : metadataStateMachine.partitions()) {
            if (partition.replicaNodes().contains(brokerId) || partition.leaderId() == brokerId) {
                reconcilePartition(partition.topic(), partition.partition());
            }
        }
    }

    private static void validateTopic(String expectedTopic, String actualTopic) {
        if (!expectedTopic.equals(actualTopic)) {
            throw new IllegalArgumentException(
                    "All partitions in createTopic must belong to the same topic. expected="
                            + expectedTopic
                            + ", actual="
                            + actualTopic);
        }
    }

    private static boolean sameLeaderAndIsr(
            ControllerPartitionMetadata current, ControllerPartitionMetadata next) {
        return current.leaderId() == next.leaderId()
                && current.isrNodes().equals(next.isrNodes())
                && java.util.Objects.equals(
                        current.truncateToLeaderEpoch(), next.truncateToLeaderEpoch())
                && java.util.Objects.equals(current.truncateToOffset(), next.truncateToOffset());
    }
}
