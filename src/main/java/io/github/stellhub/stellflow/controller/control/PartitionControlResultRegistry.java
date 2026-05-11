package io.github.stellhub.stellflow.controller.control;

import io.github.stellhub.stellflow.controlplane.PartitionControlApplyResult;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Broker 应用控制命令结果注册表。
 */
public class PartitionControlResultRegistry {

    private final Map<Integer, List<PartitionControlApplyResult>> latestReports =
            new ConcurrentHashMap<>();

    /**
     * 保存指定 broker 的最新结果上报。
     */
    public void record(int brokerId, List<PartitionControlApplyResult> results) {
        latestReports.put(brokerId, List.copyOf(results));
    }

    /**
     * 返回指定 broker 最近一次结果上报。
     */
    public List<PartitionControlApplyResult> latestReport(int brokerId) {
        return latestReports.getOrDefault(brokerId, List.of());
    }
}
