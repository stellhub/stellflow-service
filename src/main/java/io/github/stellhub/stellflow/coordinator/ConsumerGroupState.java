package io.github.stellhub.stellflow.coordinator;

/**
 * 消费组状态。
 */
public enum ConsumerGroupState {
    EMPTY,
    PREPARING_REBALANCE,
    STABLE,
    DEAD
}
