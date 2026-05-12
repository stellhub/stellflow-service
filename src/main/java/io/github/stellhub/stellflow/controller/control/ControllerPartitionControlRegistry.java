package io.github.stellhub.stellflow.controller.control;

import io.github.stellhub.stellflow.controlplane.PartitionControlCommandMessage;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Controller 侧分区控制命令注册表。
 */
public class ControllerPartitionControlRegistry {

    private final Map<Integer, List<PartitionControlCommandMessage>> commandsByBroker =
            new ConcurrentHashMap<>();
    private final Map<Integer, CopyOnWriteArrayList<PartitionCommandListener>> listenersByBroker =
            new ConcurrentHashMap<>();
    private final AtomicLong version = new AtomicLong(1);

    /**
     * 更新指定 broker 的控制命令集合并广播。
     */
    public void replaceCommands(int brokerId, List<PartitionControlCommandMessage> commands) {
        commandsByBroker.put(brokerId, List.copyOf(commands));
        long newVersion = version.incrementAndGet();
        for (PartitionCommandListener listener :
                listenersByBroker.getOrDefault(brokerId, new CopyOnWriteArrayList<>())) {
            listener.onCommands(newVersion, commandsByBroker.get(brokerId));
        }
    }

    /**
     * 返回指定 broker 当前命令快照。
     */
    public List<PartitionControlCommandMessage> commands(int brokerId) {
        return commandsByBroker.getOrDefault(brokerId, List.of());
    }

    /**
     * 注册监听器。
     */
    public long addListener(int brokerId, PartitionCommandListener listener) {
        listenersByBroker
                .computeIfAbsent(brokerId, ignored -> new CopyOnWriteArrayList<>())
                .add(listener);
        return version.get();
    }

    /**
     * 移除监听器。
     */
    public void removeListener(int brokerId, PartitionCommandListener listener) {
        listenersByBroker.getOrDefault(brokerId, new CopyOnWriteArrayList<>()).remove(listener);
    }

    /**
     * 构建 protobuf 控制命令。
     */
    public static PartitionControlCommandMessage command(
            String topic,
            int partition,
            int leaderId,
            int leaderEpoch,
            List<Integer> replicaNodes,
            List<Integer> isrNodes,
            Integer truncateToLeaderEpoch,
            Long truncateToOffset) {
        PartitionControlCommandMessage.Builder builder =
                PartitionControlCommandMessage.newBuilder()
                        .setTopic(topic)
                        .setPartition(partition)
                        .setLeaderId(leaderId)
                        .setLeaderEpoch(leaderEpoch)
                        .addAllReplicaNodes(replicaNodes)
                        .addAllIsrNodes(isrNodes);
        if (truncateToLeaderEpoch != null) {
            builder.setHasTruncateToLeaderEpoch(true);
            builder.setTruncateToLeaderEpoch(truncateToLeaderEpoch);
        }
        if (truncateToOffset != null) {
            builder.setHasTruncateToOffset(true);
            builder.setTruncateToOffset(truncateToOffset);
        }
        return builder.build();
    }

    /**
     * 构建本地删除命令。
     */
    public static PartitionControlCommandMessage deletePartitionCommand(
            String topic, int partition, int leaderEpoch) {
        return PartitionControlCommandMessage.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .setLeaderId(-1)
                .setLeaderEpoch(leaderEpoch)
                .setDeletePartition(true)
                .build();
    }

    /**
     * 简化命令回调接口。
     */
    @FunctionalInterface
    public interface PartitionCommandListener {
        void onCommands(long version, List<PartitionControlCommandMessage> commands);
    }
}
