package io.github.stellhub.stellflow.controller.replica;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 以 leader 为粒度复用连接的副本抓取连接池。
 */
public class ReplicaFetchConnectionPool implements AutoCloseable {

    private final ReplicaFetchConfig config;
    private final Map<ReplicaFetchConnectionKey, ReplicaFetchNetworkClient> clients =
            new ConcurrentHashMap<>();

    public ReplicaFetchConnectionPool(ReplicaFetchConfig config) {
        this.config = config;
    }

    /**
     * 获取指定 leader 的共享连接。
     */
    public ReplicaFetchNetworkClient get(ReplicaFetchAssignment assignment) {
        ReplicaFetchConnectionKey key =
                new ReplicaFetchConnectionKey(
                        assignment.leaderHost(), assignment.leaderPort(), assignment.leaderBrokerId());
        return clients.computeIfAbsent(key, ignored -> new ReplicaFetchNetworkClient(config, key));
    }

    /**
     * 当前活跃连接数。
     */
    public int activeConnectionCount() {
        return clients.size();
    }

    @Override
    public void close() {
        for (ReplicaFetchNetworkClient client : clients.values()) {
            try {
                client.close();
            } catch (Exception ignored) {
                // Ignore close failures during shutdown.
            }
        }
        clients.clear();
    }
}
