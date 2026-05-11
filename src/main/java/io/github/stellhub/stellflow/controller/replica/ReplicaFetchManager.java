package io.github.stellhub.stellflow.controller.replica;

import io.github.stellhub.stellflow.observability.metrics.ReplicaFetchMetrics;
import io.github.stellhub.stellflow.storage.log.LogManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * follower 后台拉取任务管理器。
 */
public class ReplicaFetchManager implements AutoCloseable {

    private final ReplicaFetchConfig config;
    private final LogManager followerLogManager;
    private final ReplicaFetchMetrics metrics;
    private final ReplicaFetchConnectionPool connectionPool;
    private final Map<String, ReplicaFetchAssignment> assignments = new ConcurrentHashMap<>();
    private final Map<String, ScheduledFuture<?>> futures = new ConcurrentHashMap<>();
    private final Map<String, ReplicaFetchTask> tasks = new ConcurrentHashMap<>();
    private final ScheduledExecutorService executorService =
            Executors.newScheduledThreadPool(
                    Math.max(4, Runtime.getRuntime().availableProcessors()),
                    runnable -> {
                        Thread thread = new Thread(runnable, "stellflow-replica-fetcher");
                        thread.setDaemon(true);
                        return thread;
                    });
    private boolean started;

    public ReplicaFetchManager(
            ReplicaFetchConfig config, LogManager followerLogManager, ReplicaFetchMetrics metrics) {
        this.config = config;
        this.followerLogManager = followerLogManager;
        this.metrics = metrics;
        this.connectionPool = new ReplicaFetchConnectionPool(config);
    }

    /**
     * 使用配置文件中的 assignments 创建管理器。
     */
    public static ReplicaFetchManager fromConfig(
            ReplicaFetchConfig config, LogManager followerLogManager, ReplicaFetchMetrics metrics) {
        ReplicaFetchManager manager = new ReplicaFetchManager(config, followerLogManager, metrics);
        for (ReplicaFetchAssignment assignment : config.parseAssignments()) {
            manager.addAssignment(assignment);
        }
        return manager;
    }

    /**
     * 新增抓取分配。
     */
    public synchronized void addAssignment(ReplicaFetchAssignment assignment) {
        String key = key(assignment.topic(), assignment.partition());
        assignments.put(key, assignment);
        if (started && !futures.containsKey(key)) {
            scheduleAssignment(key, assignment);
        }
    }

    /**
     * 全量替换当前 assignment 集。
     */
    public synchronized void replaceAssignments(List<ReplicaFetchAssignment> newAssignments) {
        Map<String, ReplicaFetchAssignment> desired = new ConcurrentHashMap<>();
        for (ReplicaFetchAssignment assignment : newAssignments) {
            desired.put(key(assignment.topic(), assignment.partition()), assignment);
        }
        Set<String> currentKeys = Set.copyOf(assignments.keySet());
        for (String currentKey : currentKeys) {
            if (!desired.containsKey(currentKey)) {
                cancelAssignment(currentKey);
                assignments.remove(currentKey);
            }
        }
        for (Map.Entry<String, ReplicaFetchAssignment> entry : desired.entrySet()) {
            ReplicaFetchAssignment existing = assignments.get(entry.getKey());
            if (existing != null && existing.equals(entry.getValue())) {
                continue;
            }
            cancelAssignment(entry.getKey());
            assignments.put(entry.getKey(), entry.getValue());
            if (started) {
                scheduleAssignment(entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * 启动所有后台抓取任务。
     */
    public synchronized void start() {
        if (!config.isEnabled() || started) {
            return;
        }
        started = true;
        for (Map.Entry<String, ReplicaFetchAssignment> entry : assignments.entrySet()) {
            scheduleAssignment(entry.getKey(), entry.getValue());
        }
    }

    private void scheduleAssignment(String key, ReplicaFetchAssignment assignment) {
        ReplicaFetchTask task =
                new ReplicaFetchTask(
                        assignment,
                        config,
                        followerLogManager,
                        new ReplicaFollowerApplier(followerLogManager),
                        connectionPool,
                        metrics);
        tasks.put(key, task);
        futures.put(
                key,
                executorService.scheduleWithFixedDelay(
                        task, 0, config.getPollIntervalMs(), TimeUnit.MILLISECONDS));
    }

    private void cancelAssignment(String key) {
        ScheduledFuture<?> future = futures.remove(key);
        if (future != null) {
            future.cancel(true);
        }
        ReplicaFetchTask task = tasks.remove(key);
        if (task != null) {
            task.close();
        }
    }

    private String key(String topic, int partition) {
        return topic + ":" + partition;
    }

    /**
     * 返回当前共享连接数量。
     */
    public int activeConnectionCount() {
        return connectionPool.activeConnectionCount();
    }

    @Override
    public synchronized void close() {
        for (String key : new ArrayList<>(futures.keySet())) {
            cancelAssignment(key);
        }
        executorService.shutdownNow();
        connectionPool.close();
    }
}
