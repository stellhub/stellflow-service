package io.github.stellhub.stellflow.controller.control;

/**
 * Controller 元数据命令提交服务。
 */
public interface ControllerMetadataCommandService extends AutoCloseable {

    /**
     * 提交 broker 注册命令。
     */
    void registerBroker(
            int brokerId,
            String advertisedEndpoint,
            String advertisedHost,
            int advertisedPort,
            long registeredAtMs);

    /**
     * 提交单分区元数据 upsert 命令。
     */
    void upsertPartition(ControllerPartitionMetadata metadata);

    /**
     * 提交单分区删除命令。
     */
    void removePartition(String topic, int partition);

    @Override
    default void close() {}
}
