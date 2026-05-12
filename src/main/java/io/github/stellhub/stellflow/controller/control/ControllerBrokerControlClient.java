package io.github.stellhub.stellflow.controller.control;

import io.github.stellhub.stellflow.controlplane.BrokerRegistrationRequest;
import io.github.stellhub.stellflow.controlplane.ControllerBrokerControlServiceGrpc;
import io.github.stellhub.stellflow.controlplane.PartitionControlApplyResult;
import io.github.stellhub.stellflow.controlplane.PartitionControlCommandEvent;
import io.github.stellhub.stellflow.controlplane.PartitionControlCommandMessage;
import io.github.stellhub.stellflow.controlplane.PartitionControlReportRequest;
import io.github.stellhub.stellflow.controlplane.PartitionControlWatchRequest;
import io.github.stellhub.stellflow.controlplane.ReplicaAssignment;
import io.github.stellhub.stellflow.controlplane.ReplicaAssignmentEvent;
import io.github.stellhub.stellflow.controlplane.ReplicaAssignmentWatchRequest;
import io.github.stellhub.stellflow.controller.replica.ControllerReplicaCoordinator;
import io.github.stellhub.stellflow.controller.replica.PartitionControlApplyReport;
import io.github.stellhub.stellflow.controller.replica.PartitionControlCommand;
import io.github.stellhub.stellflow.controller.replica.ReplicaFetchAssignment;
import io.github.stellhub.stellflow.controller.replica.ReplicaFetchManager;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Broker 侧控制面 gRPC 客户端。
 */
@Slf4j
public class ControllerBrokerControlClient implements AutoCloseable {

    private final ControlPlaneGrpcConfig config;
    private final ReplicaFetchManager replicaFetchManager;
    private final ControllerReplicaCoordinator replicaCoordinator;
    private final ManagedChannel channel;
    private final ControllerBrokerControlServiceGrpc.ControllerBrokerControlServiceStub asyncStub;
    private final ControllerBrokerControlServiceGrpc.ControllerBrokerControlServiceBlockingStub
            blockingStub;
    private final ScheduledExecutorService retryExecutor =
            Executors.newSingleThreadScheduledExecutor(
                    runnable -> {
                        Thread thread = new Thread(runnable, "stellflow-control-plane-client");
                        thread.setDaemon(true);
                        return thread;
                    });

    private volatile boolean closed;

    public ControllerBrokerControlClient(
            ControlPlaneGrpcConfig config,
            ReplicaFetchManager replicaFetchManager,
            ControllerReplicaCoordinator replicaCoordinator) {
        this.config = config;
        this.replicaFetchManager = replicaFetchManager;
        this.replicaCoordinator = replicaCoordinator;
        this.channel =
                ManagedChannelBuilder.forAddress(config.getControllerHost(), config.getControllerPort())
                        .usePlaintext()
                        .build();
        this.asyncStub = ControllerBrokerControlServiceGrpc.newStub(channel);
        this.blockingStub = ControllerBrokerControlServiceGrpc.newBlockingStub(channel);
    }

    /**
     * 启动 broker 注册、assignment watch 和 partition control watch。
     */
    public void start() {
        if (!config.isClientEnabled()) {
            return;
        }
        registerBroker();
        retryExecutor.scheduleWithFixedDelay(
                this::safeRegisterBroker,
                config.getRegistrationIntervalMs(),
                config.getRegistrationIntervalMs(),
                TimeUnit.MILLISECONDS);
        subscribeAssignments();
        subscribePartitionControlCommands();
    }

    private void safeRegisterBroker() {
        if (closed) {
            return;
        }
        try {
            registerBroker();
        } catch (RuntimeException exception) {
            if (!closed) {
                log.warn("Failed to refresh broker registration heartbeat", exception);
            }
        }
    }

    private void registerBroker() {
        blockingStub.registerBroker(
                BrokerRegistrationRequest.newBuilder()
                        .setBrokerId(config.getBrokerId())
                        .setAdvertisedHost(config.getAdvertisedHost())
                        .setAdvertisedPort(config.getAdvertisedPort())
                        .build());
    }

    private void subscribeAssignments() {
        if (closed) {
            return;
        }
        asyncStub.watchReplicaAssignments(
                ReplicaAssignmentWatchRequest.newBuilder().setBrokerId(config.getBrokerId()).build(),
                new StreamObserver<>() {
                    @Override
                    public void onNext(ReplicaAssignmentEvent value) {
                        List<ReplicaFetchAssignment> assignments =
                                value.getAssignmentsList().stream()
                                        .map(ControllerBrokerControlClient::toAssignment)
                                        .collect(Collectors.toList());
                        replicaFetchManager.replaceAssignments(assignments);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        if (closed) {
                            return;
                        }
                        log.warn("Control plane watch stream failed, will retry", throwable);
                        retryExecutor.schedule(
                                ControllerBrokerControlClient.this::subscribeAssignments,
                                config.getWatchReconnectBackoffMs(),
                                TimeUnit.MILLISECONDS);
                    }

                    @Override
                    public void onCompleted() {
                        if (closed) {
                            return;
                        }
                        retryExecutor.schedule(
                                ControllerBrokerControlClient.this::subscribeAssignments,
                                config.getWatchReconnectBackoffMs(),
                                TimeUnit.MILLISECONDS);
                    }
                });
    }

    private static ReplicaFetchAssignment toAssignment(ReplicaAssignment assignment) {
        return new ReplicaFetchAssignment(
                assignment.getTopic(),
                assignment.getPartition(),
                assignment.getLeaderHost(),
                assignment.getLeaderPort(),
                assignment.getLeaderBrokerId());
    }

    private void subscribePartitionControlCommands() {
        if (closed) {
            return;
        }
        asyncStub.watchPartitionControlCommands(
                PartitionControlWatchRequest.newBuilder().setBrokerId(config.getBrokerId()).build(),
                new StreamObserver<>() {
                    @Override
                    public void onNext(PartitionControlCommandEvent value) {
                        List<PartitionControlApplyResult> reports =
                                value.getCommandsList().stream()
                                        .map(ControllerBrokerControlClient::toCommand)
                                        .map(replicaCoordinator::apply)
                                        .map(ControllerBrokerControlClient::toReport)
                                        .collect(Collectors.toList());
                        blockingStub.reportPartitionControlResults(
                                PartitionControlReportRequest.newBuilder()
                                        .setBrokerId(config.getBrokerId())
                                        .addAllResults(reports)
                                        .build());
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        if (closed) {
                            return;
                        }
                        log.warn("Partition control watch stream failed, will retry", throwable);
                        retryExecutor.schedule(
                                ControllerBrokerControlClient.this::subscribePartitionControlCommands,
                                config.getWatchReconnectBackoffMs(),
                                TimeUnit.MILLISECONDS);
                    }

                    @Override
                    public void onCompleted() {
                        if (closed) {
                            return;
                        }
                        retryExecutor.schedule(
                                ControllerBrokerControlClient.this::subscribePartitionControlCommands,
                                config.getWatchReconnectBackoffMs(),
                                TimeUnit.MILLISECONDS);
                    }
                });
    }

    private static PartitionControlCommand toCommand(PartitionControlCommandMessage command) {
        return new PartitionControlCommand(
                command.getTopic(),
                command.getPartition(),
                command.getLeaderId(),
                command.getLeaderEpoch(),
                command.getReplicaNodesList(),
                command.getIsrNodesList(),
                command.getHasTruncateToLeaderEpoch() ? command.getTruncateToLeaderEpoch() : null,
                command.getHasTruncateToOffset() ? command.getTruncateToOffset() : null,
                command.getDeletePartition());
    }

    private static PartitionControlApplyResult toReport(PartitionControlApplyReport report) {
        return PartitionControlApplyResult.newBuilder()
                .setTopic(report.topic())
                .setPartition(report.partition())
                .setLeaderEpoch(report.leaderEpoch())
                .setSuccess(report.success())
                .setMessage(report.message() == null ? "" : report.message())
                .setAppliedAtMs(report.appliedAtMs())
                .setDeletePartition(report.deletePartition())
                .build();
    }

    @Override
    public void close() {
        closed = true;
        retryExecutor.shutdownNow();
        channel.shutdownNow();
    }
}
