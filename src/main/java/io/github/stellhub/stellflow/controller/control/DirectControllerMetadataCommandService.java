package io.github.stellhub.stellflow.controller.control;

/**
 * 直接以内存状态机提交元数据命令的实现。
 */
public class DirectControllerMetadataCommandService implements ControllerMetadataCommandService {

    private final ControllerMetadataStateMachine metadataStateMachine;

    public DirectControllerMetadataCommandService(ControllerMetadataStateMachine metadataStateMachine) {
        this.metadataStateMachine = metadataStateMachine;
    }

    @Override
    public void registerBroker(
            int brokerId,
            String advertisedEndpoint,
            String advertisedHost,
            int advertisedPort,
            long registeredAtMs) {
        metadataStateMachine.registerBroker(
                brokerId, advertisedEndpoint, advertisedHost, advertisedPort, registeredAtMs);
    }

    @Override
    public void upsertPartition(ControllerPartitionMetadata metadata) {
        metadataStateMachine.upsertPartition(metadata);
    }

    @Override
    public void removePartition(String topic, int partition) {
        metadataStateMachine.removePartition(topic, partition);
    }
}
