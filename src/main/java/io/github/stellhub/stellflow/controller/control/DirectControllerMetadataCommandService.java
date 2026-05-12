package io.github.stellhub.stellflow.controller.control;

import io.github.stellhub.stellflow.controller.quorum.ControllerMetadataRecord;
import io.github.stellhub.stellflow.controller.quorum.ControllerMetadataRecordType;

/**
 * 直接以内存状态机提交元数据命令的实现。
 */
public class DirectControllerMetadataCommandService implements ControllerMetadataCommandService {

    private final ControllerMetadataStateMachine metadataStateMachine;

    public DirectControllerMetadataCommandService(ControllerMetadataStateMachine metadataStateMachine) {
        this.metadataStateMachine = metadataStateMachine;
    }

    @Override
    public void submit(ControllerMetadataRecord record) {
        switch (record.type()) {
            case REGISTER_BROKER ->
                    metadataStateMachine.registerBroker(
                            record.brokerId(),
                            record.advertisedEndpoint(),
                            record.advertisedHost(),
                            record.advertisedPort(),
                            record.registeredAtMs());
            case FENCE_BROKER -> metadataStateMachine.fenceBroker(record.brokerId());
            case UNFENCE_BROKER -> metadataStateMachine.unfenceBroker(record.brokerId());
            case CREATE_TOPIC ->
                    metadataStateMachine.createTopic(
                            record.topic(), record.partitionCount(), record.topicCreatedAtMs());
            case DELETE_TOPIC -> metadataStateMachine.deleteTopic(record.topic());
            case EXPAND_TOPIC_PARTITIONS ->
                    metadataStateMachine.expandTopicPartitions(
                            record.topic(), record.partitionCount());
            case UPDATE_PARTITION_TOPOLOGY ->
                    metadataStateMachine.updatePartitionTopology(
                            record.topic(), record.partition(), record.replicaNodes());
            case UPDATE_PARTITION_LEADER_ISR ->
                    metadataStateMachine.updatePartitionLeaderAndIsr(
                            record.topic(),
                            record.partition(),
                            record.leaderId(),
                            record.leaderEpoch(),
                            record.isrNodes(),
                            record.truncateToLeaderEpoch(),
                            record.truncateToOffset());
            case SHRINK_PARTITION_ISR ->
                    metadataStateMachine.shrinkPartitionIsr(
                            record.topic(),
                            record.partition(),
                            record.brokerId(),
                            record.leaderEpoch());
            case EXPAND_PARTITION_ISR ->
                    metadataStateMachine.expandPartitionIsr(
                            record.topic(),
                            record.partition(),
                            record.brokerId(),
                            record.leaderEpoch());
            case REMOVE_PARTITION ->
                    {
                        if (record.partition() != null && record.partition() >= 0) {
                            metadataStateMachine.removePartition(record.topic(), record.partition());
                        }
                    }
        }
    }
}
