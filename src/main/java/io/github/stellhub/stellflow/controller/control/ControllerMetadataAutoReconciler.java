package io.github.stellhub.stellflow.controller.control;

import io.github.stellhub.stellflow.controller.replica.ControllerReplicaCoordinator;
import io.github.stellhub.stellflow.controller.replica.PartitionReplicaProgress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * Controller 后台自动收敛任务。
 */
@Slf4j
public class ControllerMetadataAutoReconciler implements AutoCloseable {

    private final ControllerAutoReconcileConfig config;
    private final ControllerMetadataStateMachine metadataStateMachine;
    private final ControllerMetadataDecisionService metadataDecisionService;
    private final ControllerReplicaCoordinator replicaCoordinator;
    private final ScheduledExecutorService executorService =
            Executors.newSingleThreadScheduledExecutor(
                    runnable -> {
                        Thread thread = new Thread(runnable, "stellflow-controller-reconciler");
                        thread.setDaemon(true);
                        return thread;
                    });
    private volatile boolean started;

    public ControllerMetadataAutoReconciler(
            ControllerAutoReconcileConfig config,
            ControllerMetadataStateMachine metadataStateMachine,
            ControllerMetadataDecisionService metadataDecisionService,
            ControllerReplicaCoordinator replicaCoordinator) {
        this.config = config;
        this.metadataStateMachine = metadataStateMachine;
        this.metadataDecisionService = metadataDecisionService;
        this.replicaCoordinator = replicaCoordinator;
    }

    /**
     * 启动后台自动收敛。
     */
    public synchronized void start() {
        if (!config.isEnabled() || started) {
            return;
        }
        started = true;
        executorService.scheduleWithFixedDelay(
                this::safeReconcile, config.getIntervalMs(), config.getIntervalMs(), TimeUnit.MILLISECONDS);
    }

    /**
     * 执行一次同步收敛。
     */
    public void reconcileOnce() {
        reconcileBrokerLiveness();
        reconcilePartitions();
    }

    private void safeReconcile() {
        try {
            reconcileOnce();
        } catch (RuntimeException exception) {
            log.warn("Controller auto reconcile failed", exception);
        }
    }

    private void reconcileBrokerLiveness() {
        long now = System.currentTimeMillis();
        for (BrokerRegistrationMetadata broker : metadataStateMachine.brokers()) {
            long elapsed = now - broker.registeredAtMs();
            if (elapsed > config.getBrokerHeartbeatTimeoutMs()) {
                if (!broker.fenced()) {
                    metadataDecisionService.fenceBroker(broker.brokerId());
                }
                continue;
            }
            if (broker.fenced()) {
                metadataDecisionService.unfenceBroker(broker.brokerId());
            }
        }
    }

    private void reconcilePartitions() {
        for (ControllerPartitionMetadata partition : metadataStateMachine.partitions()) {
            reconcilePartition(partition);
        }
    }

    private void reconcilePartition(ControllerPartitionMetadata current) {
        ControllerPartitionMetadata livenessCandidate =
                new ControllerMetadataPlanner()
                        .reconcilePartition(current, metadataStateMachine.brokers());
        Optional<PartitionReplicaProgress> progressOptional =
                replicaCoordinator.partitionProgress(current.topic(), current.partition());
        if (progressOptional.isEmpty()) {
            applyIfChanged(current, livenessCandidate);
            return;
        }

        PartitionReplicaProgress progress = progressOptional.get();
        long leaderLogEndOffset = progress.logEndOffset();
        List<Integer> desiredIsr = new ArrayList<>();
        for (Integer replicaId : livenessCandidate.replicaNodes()) {
            if (replicaId == livenessCandidate.leaderId()) {
                desiredIsr.add(replicaId);
                continue;
            }
            if (metadataStateMachine.broker(replicaId).map(BrokerRegistrationMetadata::fenced).orElse(true)) {
                continue;
            }
            long replicaEndOffset =
                    replicaCoordinator.replicaEndOffset(current.topic(), current.partition(), replicaId);
            long lag = Math.max(0, leaderLogEndOffset - replicaEndOffset);
            if (lag <= config.getMaxReplicaLagMessages()) {
                desiredIsr.add(replicaId);
            }
        }
        if (desiredIsr.isEmpty()) {
            desiredIsr.add(livenessCandidate.leaderId());
        }
        ControllerPartitionMetadata next =
                ControllerMetadataStateMachine.partition(
                        livenessCandidate.topic(),
                        livenessCandidate.partition(),
                        livenessCandidate.leaderId(),
                        livenessCandidate.leaderEpoch(),
                        livenessCandidate.replicaNodes(),
                        List.copyOf(desiredIsr),
                        livenessCandidate.truncateToLeaderEpoch(),
                        livenessCandidate.truncateToOffset());
        applyIfChanged(current, next);
    }

    private void applyIfChanged(
            ControllerPartitionMetadata current, ControllerPartitionMetadata next) {
        if (current.leaderId() == next.leaderId()
                && current.isrNodes().equals(next.isrNodes())) {
            return;
        }
        metadataDecisionService.changeLeaderAndIsr(
                current.topic(),
                current.partition(),
                next.leaderId(),
                Math.max(current.leaderEpoch() + 1, next.leaderEpoch()),
                next.isrNodes(),
                next.truncateToLeaderEpoch(),
                next.truncateToOffset());
    }

    @Override
    public synchronized void close() {
        executorService.shutdownNow();
    }
}
