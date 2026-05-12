package io.github.stellhub.stellflow.controller.quorum;

import io.github.stellhub.stellflow.controller.control.ControllerPartitionMetadata;
import java.util.List;
import lombok.Builder;

/**
 * Controller 元数据日志记录。
 */
@Builder
public record ControllerMetadataRecord(
        ControllerMetadataRecordType type,
        Integer brokerId,
        String advertisedEndpoint,
        String advertisedHost,
        Integer advertisedPort,
        Long registeredAtMs,
        String topic,
        Integer partitionCount,
        Long topicCreatedAtMs,
        Integer partition,
        Integer leaderId,
        Integer leaderEpoch,
        List<Integer> replicaNodes,
        List<Integer> isrNodes,
        Integer truncateToLeaderEpoch,
        Long truncateToOffset) {

    /**
     * 构造 broker 注册记录。
     */
    public static ControllerMetadataRecord registerBroker(
            int brokerId,
            String advertisedEndpoint,
            String advertisedHost,
            int advertisedPort,
            long registeredAtMs) {
        return ControllerMetadataRecord.builder()
                .type(ControllerMetadataRecordType.REGISTER_BROKER)
                .brokerId(brokerId)
                .advertisedEndpoint(advertisedEndpoint)
                .advertisedHost(advertisedHost)
                .advertisedPort(advertisedPort)
                .registeredAtMs(registeredAtMs)
                .build();
    }

    /**
     * 构造 broker fenced 记录。
     */
    public static ControllerMetadataRecord fenceBroker(int brokerId) {
        return ControllerMetadataRecord.builder()
                .type(ControllerMetadataRecordType.FENCE_BROKER)
                .brokerId(brokerId)
                .build();
    }

    /**
     * 构造 broker unfenced 记录。
     */
    public static ControllerMetadataRecord unfenceBroker(int brokerId) {
        return ControllerMetadataRecord.builder()
                .type(ControllerMetadataRecordType.UNFENCE_BROKER)
                .brokerId(brokerId)
                .build();
    }

    /**
     * 构造 topic 创建记录。
     */
    public static ControllerMetadataRecord createTopic(
            String topic, int partitionCount, long topicCreatedAtMs) {
        return ControllerMetadataRecord.builder()
                .type(ControllerMetadataRecordType.CREATE_TOPIC)
                .topic(topic)
                .partitionCount(partitionCount)
                .topicCreatedAtMs(topicCreatedAtMs)
                .build();
    }

    /**
     * 构造 topic 删除记录。
     */
    public static ControllerMetadataRecord deleteTopic(String topic) {
        return ControllerMetadataRecord.builder()
                .type(ControllerMetadataRecordType.DELETE_TOPIC)
                .topic(topic)
                .build();
    }

    /**
     * 构造 topic 分区扩容记录。
     */
    public static ControllerMetadataRecord expandTopicPartitions(String topic, int partitionCount) {
        return ControllerMetadataRecord.builder()
                .type(ControllerMetadataRecordType.EXPAND_TOPIC_PARTITIONS)
                .topic(topic)
                .partitionCount(partitionCount)
                .build();
    }

    /**
     * 构造分区拓扑变更记录。
     */
    public static ControllerMetadataRecord updatePartitionTopology(
            String topic, int partition, List<Integer> replicaNodes) {
        return ControllerMetadataRecord.builder()
                .type(ControllerMetadataRecordType.UPDATE_PARTITION_TOPOLOGY)
                .topic(topic)
                .partition(partition)
                .replicaNodes(replicaNodes)
                .build();
    }

    /**
     * 构造分区 leader 与 ISR 变更记录。
     */
    public static ControllerMetadataRecord updatePartitionLeaderAndIsr(
            String topic,
            int partition,
            int leaderId,
            int leaderEpoch,
            List<Integer> isrNodes,
            Integer truncateToLeaderEpoch,
            Long truncateToOffset) {
        return ControllerMetadataRecord.builder()
                .type(ControllerMetadataRecordType.UPDATE_PARTITION_LEADER_ISR)
                .topic(topic)
                .partition(partition)
                .leaderId(leaderId)
                .leaderEpoch(leaderEpoch)
                .isrNodes(isrNodes)
                .truncateToLeaderEpoch(truncateToLeaderEpoch)
                .truncateToOffset(truncateToOffset)
                .build();
    }

    /**
     * 构造 ISR shrink 记录。
     */
    public static ControllerMetadataRecord shrinkPartitionIsr(
            String topic, int partition, int brokerId, int leaderEpoch) {
        return ControllerMetadataRecord.builder()
                .type(ControllerMetadataRecordType.SHRINK_PARTITION_ISR)
                .topic(topic)
                .partition(partition)
                .brokerId(brokerId)
                .leaderEpoch(leaderEpoch)
                .build();
    }

    /**
     * 构造 ISR expand 记录。
     */
    public static ControllerMetadataRecord expandPartitionIsr(
            String topic, int partition, int brokerId, int leaderEpoch) {
        return ControllerMetadataRecord.builder()
                .type(ControllerMetadataRecordType.EXPAND_PARTITION_ISR)
                .topic(topic)
                .partition(partition)
                .brokerId(brokerId)
                .leaderEpoch(leaderEpoch)
                .build();
    }

    /**
     * 构造分区删除记录。
     */
    public static ControllerMetadataRecord removePartition(String topic, int partition) {
        return ControllerMetadataRecord.builder()
                .type(ControllerMetadataRecordType.REMOVE_PARTITION)
                .topic(topic)
                .partition(partition)
                .build();
    }
}
