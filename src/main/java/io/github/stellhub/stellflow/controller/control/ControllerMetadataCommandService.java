package io.github.stellhub.stellflow.controller.control;

import io.github.stellhub.stellflow.controller.quorum.ControllerMetadataRecord;
import io.github.stellhub.stellflow.controller.quorum.ControllerMetadataRecordType;
import java.util.List;

/**
 * Controller 元数据命令提交服务。
 */
public interface ControllerMetadataCommandService extends AutoCloseable {

    /**
     * 提交单条元数据记录。
     */
    void submit(ControllerMetadataRecord record);

    /**
     * 批量提交元数据记录。
     */
    default void submitAll(List<ControllerMetadataRecord> records) {
        for (ControllerMetadataRecord record : records) {
            submit(record);
        }
    }

    /**
     * 提交 broker 注册命令。
     */
    default void registerBroker(
            int brokerId,
            String advertisedEndpoint,
            String advertisedHost,
            int advertisedPort,
            long registeredAtMs) {
        submit(
                ControllerMetadataRecord.registerBroker(
                        brokerId, advertisedEndpoint, advertisedHost, advertisedPort, registeredAtMs));
    }

    /**
     * 提交 topic 创建命令。
     */
    default void createTopic(String topic, int partitionCount, long createdAtMs) {
        submit(ControllerMetadataRecord.createTopic(topic, partitionCount, createdAtMs));
    }

    /**
     * 提交 topic 删除命令。
     */
    default void deleteTopic(String topic) {
        submit(ControllerMetadataRecord.deleteTopic(topic));
    }

    /**
     * 提交 topic 分区扩容命令。
     */
    default void expandTopicPartitions(String topic, int partitionCount) {
        submit(ControllerMetadataRecord.expandTopicPartitions(topic, partitionCount));
    }

    /**
     * 提交 broker fenced 命令。
     */
    default void fenceBroker(int brokerId) {
        submit(ControllerMetadataRecord.fenceBroker(brokerId));
    }

    /**
     * 提交 broker unfenced 命令。
     */
    default void unfenceBroker(int brokerId) {
        submit(ControllerMetadataRecord.unfenceBroker(brokerId));
    }

    /**
     * 提交分区拓扑变更命令。
     */
    default void updatePartitionTopology(String topic, int partition, List<Integer> replicaNodes) {
        submit(ControllerMetadataRecord.updatePartitionTopology(topic, partition, replicaNodes));
    }

    /**
     * 提交 leader 与 ISR 变更命令。
     */
    default void updatePartitionLeaderAndIsr(
            String topic,
            int partition,
            int leaderId,
            int leaderEpoch,
            List<Integer> isrNodes,
            Integer truncateToLeaderEpoch,
            Long truncateToOffset) {
        submit(
                ControllerMetadataRecord.updatePartitionLeaderAndIsr(
                        topic,
                        partition,
                        leaderId,
                        leaderEpoch,
                        isrNodes,
                        truncateToLeaderEpoch,
                        truncateToOffset));
    }

    /**
     * 提交 ISR shrink 命令。
     */
    default void shrinkPartitionIsr(String topic, int partition, int brokerId, int leaderEpoch) {
        submit(ControllerMetadataRecord.shrinkPartitionIsr(topic, partition, brokerId, leaderEpoch));
    }

    /**
     * 提交 ISR expand 命令。
     */
    default void expandPartitionIsr(String topic, int partition, int brokerId, int leaderEpoch) {
        submit(ControllerMetadataRecord.expandPartitionIsr(topic, partition, brokerId, leaderEpoch));
    }

    /**
     * 提交单分区删除命令。
     */
    default void removePartition(String topic, int partition) {
        submit(ControllerMetadataRecord.removePartition(topic, partition));
    }

    @Override
    default void close() {}
}
