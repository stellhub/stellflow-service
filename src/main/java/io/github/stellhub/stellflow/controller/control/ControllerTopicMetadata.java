package io.github.stellhub.stellflow.controller.control;

/**
 * Controller 侧 topic 元数据。
 */
public record ControllerTopicMetadata(String topic, int partitionCount, long createdAtMs) {}
