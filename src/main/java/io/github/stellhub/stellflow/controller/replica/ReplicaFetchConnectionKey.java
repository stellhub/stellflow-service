package io.github.stellhub.stellflow.controller.replica;

/**
 * 按 leader 维度复用的副本抓取连接键。
 */
public record ReplicaFetchConnectionKey(String leaderHost, int leaderPort, int leaderBrokerId) {}
