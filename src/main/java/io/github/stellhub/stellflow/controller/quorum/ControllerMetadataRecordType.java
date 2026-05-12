package io.github.stellhub.stellflow.controller.quorum;

/**
 * Controller 元数据日志记录类型。
 */
public enum ControllerMetadataRecordType {
    REGISTER_BROKER,
    FENCE_BROKER,
    UNFENCE_BROKER,
    CREATE_TOPIC,
    DELETE_TOPIC,
    EXPAND_TOPIC_PARTITIONS,
    UPDATE_PARTITION_TOPOLOGY,
    UPDATE_PARTITION_LEADER_ISR,
    SHRINK_PARTITION_ISR,
    EXPAND_PARTITION_ISR,
    REMOVE_PARTITION
}
