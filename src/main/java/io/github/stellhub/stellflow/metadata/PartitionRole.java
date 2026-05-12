package io.github.stellhub.stellflow.metadata;

/**
 * Broker 本地分区角色。
 */
public enum PartitionRole {
    LEADER,
    FOLLOWER,
    OFFLINE,
    DELETING
}
