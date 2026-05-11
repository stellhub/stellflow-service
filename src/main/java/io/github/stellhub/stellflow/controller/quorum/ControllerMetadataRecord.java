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
     * 构造分区 upsert 记录。
     */
    public static ControllerMetadataRecord upsertPartition(ControllerPartitionMetadata metadata) {
        return ControllerMetadataRecord.builder()
                .type(ControllerMetadataRecordType.UPSERT_PARTITION)
                .topic(metadata.topic())
                .partition(metadata.partition())
                .leaderId(metadata.leaderId())
                .leaderEpoch(metadata.leaderEpoch())
                .replicaNodes(metadata.replicaNodes())
                .isrNodes(metadata.isrNodes())
                .truncateToLeaderEpoch(metadata.truncateToLeaderEpoch())
                .truncateToOffset(metadata.truncateToOffset())
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
