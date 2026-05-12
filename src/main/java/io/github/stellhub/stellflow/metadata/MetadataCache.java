package io.github.stellhub.stellflow.metadata;

import io.github.stellhub.stellflow.controlplane.PartitionControlCommandMessage;
import io.github.stellhub.stellflow.controlplane.ReplicaAssignment;
import io.github.stellhub.stellflow.storage.log.LogManager;
import io.github.stellhub.stellflow.storage.log.TopicPartition;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Broker 本地元数据缓存。
 */
public class MetadataCache {

    private final int localBrokerId;
    private final Map<Integer, BrokerEndpoint> brokers = new ConcurrentHashMap<>();
    private final Map<TopicPartition, PartitionMetadata> partitions = new ConcurrentHashMap<>();

    public MetadataCache(int localBrokerId) {
        this.localBrokerId = localBrokerId;
    }

    /**
     * 注册本地 broker 端点。
     */
    public void registerLocalBroker(String host, int port) {
        brokers.put(localBrokerId, new BrokerEndpoint(localBrokerId, host, port, null));
    }

    /**
     * 应用副本抓取 assignment 中携带的 leader 端点。
     */
    public void applyReplicaAssignments(List<ReplicaAssignment> assignments) {
        for (ReplicaAssignment assignment : assignments) {
            brokers.put(
                    assignment.getLeaderBrokerId(),
                    new BrokerEndpoint(
                            assignment.getLeaderBrokerId(),
                            assignment.getLeaderHost(),
                            assignment.getLeaderPort(),
                            null));
        }
    }

    /**
     * 应用 controller 下发给当前 broker 的分区控制快照。
     */
    public void applyPartitionControlSnapshot(List<PartitionControlCommandMessage> commands) {
        Set<TopicPartition> nextKeys = new TreeSet<>(Comparator.comparing(TopicPartition::topic)
                .thenComparingInt(TopicPartition::partition));
        for (PartitionControlCommandMessage command : commands) {
            TopicPartition topicPartition = new TopicPartition(command.getTopic(), command.getPartition());
            if (command.getDeletePartition()) {
                partitions.remove(topicPartition);
                continue;
            }
            nextKeys.add(topicPartition);
            partitions.put(
                    topicPartition,
                    new PartitionMetadata(
                            command.getTopic(),
                            command.getPartition(),
                            command.getLeaderId(),
                            command.getLeaderEpoch(),
                            command.getReplicaNodesList(),
                            command.getIsrNodesList(),
                            roleFor(command.getLeaderId(), command.getReplicaNodesList())));
        }
        partitions.keySet().removeIf(key -> belongsToLocalBroker(key) && !nextKeys.contains(key));
    }

    /**
     * 直接创建本地 topic 元数据，主要用于单机开发和 Admin API。
     */
    public void createLocalTopic(String topic, int partitionCount) {
        for (int partition = 0; partition < partitionCount; partition++) {
            upsertPartition(
                    new PartitionMetadata(
                            topic,
                            partition,
                            localBrokerId,
                            0,
                            List.of(localBrokerId),
                            List.of(localBrokerId),
                            PartitionRole.LEADER));
        }
    }

    /**
     * 插入或更新单个分区元数据。
     */
    public void upsertPartition(PartitionMetadata metadata) {
        partitions.put(new TopicPartition(metadata.topic(), metadata.partition()), metadata);
    }

    /**
     * 删除本地 topic 元数据。
     */
    public void deleteTopic(String topic) {
        partitions.keySet().removeIf(topicPartition -> topicPartition.topic().equals(topic));
    }

    /**
     * 删除单个分区元数据。
     */
    public void deletePartition(String topic, int partition) {
        partitions.remove(new TopicPartition(topic, partition));
    }

    /**
     * 查询分区元数据。
     */
    public Optional<PartitionMetadata> partition(String topic, int partition) {
        return Optional.ofNullable(partitions.get(new TopicPartition(topic, partition)));
    }

    /**
     * 判断 topic 是否存在于缓存。
     */
    public boolean containsTopic(String topic) {
        return partitions.keySet().stream().anyMatch(key -> key.topic().equals(topic));
    }

    /**
     * 返回 topic 名称快照。
     */
    public Set<String> topicNames() {
        Set<String> values = new TreeSet<>();
        for (TopicPartition topicPartition : partitions.keySet()) {
            values.add(topicPartition.topic());
        }
        return values;
    }

    /**
     * 返回 topic 下的分区元数据。
     */
    public List<PartitionMetadata> topicPartitions(String topic) {
        return partitions.values().stream()
                .filter(metadata -> metadata.topic().equals(topic))
                .sorted(Comparator.comparingInt(PartitionMetadata::partition))
                .toList();
    }

    /**
     * 返回 broker 端点快照。
     */
    public List<BrokerEndpoint> brokers() {
        return brokers.values().stream()
                .sorted(Comparator.comparingInt(BrokerEndpoint::brokerId))
                .toList();
    }

    /**
     * 从已有本地日志恢复出最小元数据视图。
     */
    public void bootstrapFromLogManager(LogManager logManager) {
        for (String topic : logManager.topicNames()) {
            for (Integer partition : logManager.partitions(topic)) {
                upsertPartition(
                        new PartitionMetadata(
                                topic,
                                partition,
                                logManager.leaderId(topic, partition),
                                logManager.leaderEpoch(topic, partition),
                                logManager.replicaNodes(topic, partition),
                                logManager.isrNodes(topic, partition),
                                roleFor(
                                        logManager.leaderId(topic, partition),
                                        logManager.replicaNodes(topic, partition))));
            }
        }
    }

    /**
     * 返回当前 brokerId。
     */
    public int localBrokerId() {
        return localBrokerId;
    }

    private boolean belongsToLocalBroker(TopicPartition topicPartition) {
        PartitionMetadata current = partitions.get(topicPartition);
        return current != null && current.replicaNodes().contains(localBrokerId);
    }

    private PartitionRole roleFor(int leaderId, List<Integer> replicaNodes) {
        if (leaderId == localBrokerId) {
            return PartitionRole.LEADER;
        }
        if (replicaNodes != null && replicaNodes.contains(localBrokerId)) {
            return PartitionRole.FOLLOWER;
        }
        return PartitionRole.OFFLINE;
    }
}
