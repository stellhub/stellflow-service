package io.github.stellhub.stellflow.controller.control;

import io.github.stellhub.stellflow.controlplane.ReplicaAssignment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Controller 侧副本抓取分配注册表。
 */
public class ControllerAssignmentRegistry {

    private final Map<Integer, List<ReplicaAssignment>> assignmentsByBroker = new ConcurrentHashMap<>();
    private final Map<Integer, CopyOnWriteArrayList<AssignmentListener>> listenersByBroker =
            new ConcurrentHashMap<>();
    private final AtomicLong version = new AtomicLong(1);

    /**
     * 更新指定 broker 的 assignment 列表并广播。
     */
    public void replaceAssignments(int brokerId, List<ReplicaAssignment> assignments) {
        assignmentsByBroker.put(brokerId, List.copyOf(assignments));
        long newVersion = version.incrementAndGet();
        for (AssignmentListener listener :
                listenersByBroker.getOrDefault(brokerId, new CopyOnWriteArrayList<>())) {
            listener.onAssignments(newVersion, assignmentsByBroker.get(brokerId));
        }
    }

    /**
     * 返回指定 broker 当前快照。
     */
    public List<ReplicaAssignment> assignments(int brokerId) {
        return assignmentsByBroker.getOrDefault(brokerId, List.of());
    }

    /**
     * 注册监听器，并立即返回当前快照版本。
     */
    public long addListener(int brokerId, AssignmentListener listener) {
        listenersByBroker
                .computeIfAbsent(brokerId, ignored -> new CopyOnWriteArrayList<>())
                .add(listener);
        return version.get();
    }

    /**
     * 移除监听器。
     */
    public void removeListener(int brokerId, AssignmentListener listener) {
        listenersByBroker
                .getOrDefault(brokerId, new CopyOnWriteArrayList<>())
                .remove(listener);
    }

    /**
     * 构建 protobuf assignment。
     */
    public static ReplicaAssignment assignment(
            String topic, int partition, String leaderHost, int leaderPort, int leaderBrokerId) {
        return ReplicaAssignment.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .setLeaderHost(leaderHost)
                .setLeaderPort(leaderPort)
                .setLeaderBrokerId(leaderBrokerId)
                .build();
    }

    /**
     * 简化回调接口。
     */
    @FunctionalInterface
    public interface AssignmentListener {
        void onAssignments(long version, List<ReplicaAssignment> assignments);
    }
}
