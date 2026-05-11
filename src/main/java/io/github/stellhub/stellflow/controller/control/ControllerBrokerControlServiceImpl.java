package io.github.stellhub.stellflow.controller.control;

import io.github.stellhub.stellflow.controlplane.BrokerRegistrationRequest;
import io.github.stellhub.stellflow.controlplane.BrokerRegistrationResponse;
import io.github.stellhub.stellflow.controlplane.ControllerBrokerControlServiceGrpc;
import io.github.stellhub.stellflow.controlplane.PartitionControlApplyResult;
import io.github.stellhub.stellflow.controlplane.PartitionControlBatchRequest;
import io.github.stellhub.stellflow.controlplane.PartitionControlBatchResponse;
import io.github.stellhub.stellflow.controlplane.PartitionControlCommandMessage;
import io.github.stellhub.stellflow.controlplane.PartitionControlCommandEvent;
import io.github.stellhub.stellflow.controlplane.PartitionControlReportRequest;
import io.github.stellhub.stellflow.controlplane.PartitionControlReportResponse;
import io.github.stellhub.stellflow.controlplane.PartitionControlWatchRequest;
import io.github.stellhub.stellflow.controlplane.ReplicaAssignmentEvent;
import io.github.stellhub.stellflow.controlplane.ReplicaAssignmentWatchRequest;
import io.github.stellhub.stellflow.controller.replica.ControllerReplicaCoordinator;
import io.github.stellhub.stellflow.controller.replica.PartitionControlCommand;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * Controller/Broker gRPC 控制面服务实现。
 */
@Slf4j
public class ControllerBrokerControlServiceImpl
        extends ControllerBrokerControlServiceGrpc.ControllerBrokerControlServiceImplBase {

    private final ControllerAssignmentRegistry assignmentRegistry;
    private final ControllerPartitionControlRegistry partitionControlRegistry;
    private final PartitionControlResultRegistry partitionControlResultRegistry;
    private final ControllerMetadataCommandService metadataCommandService;
    private final ControllerMetadataStateMachine metadataStateMachine;
    private final ControllerReplicaCoordinator replicaCoordinator;
    private final String clusterId;

    public ControllerBrokerControlServiceImpl(
            ControllerAssignmentRegistry assignmentRegistry,
            ControllerPartitionControlRegistry partitionControlRegistry,
            PartitionControlResultRegistry partitionControlResultRegistry,
            ControllerMetadataCommandService metadataCommandService,
            ControllerMetadataStateMachine metadataStateMachine,
            ControllerReplicaCoordinator replicaCoordinator,
            String clusterId) {
        this.assignmentRegistry = assignmentRegistry;
        this.partitionControlRegistry = partitionControlRegistry;
        this.partitionControlResultRegistry = partitionControlResultRegistry;
        this.metadataCommandService = metadataCommandService;
        this.metadataStateMachine = metadataStateMachine;
        this.replicaCoordinator = replicaCoordinator;
        this.clusterId = clusterId;
    }

    @Override
    public void registerBroker(
            BrokerRegistrationRequest request,
            StreamObserver<BrokerRegistrationResponse> responseObserver) {
        log.info(
                "Broker registered brokerId={} host={} port={}",
                request.getBrokerId(),
                request.getAdvertisedHost(),
                request.getAdvertisedPort());
        try {
            metadataCommandService.registerBroker(
                    request.getBrokerId(),
                    "stellflow://"
                            + request.getAdvertisedHost()
                            + ":"
                            + request.getAdvertisedPort(),
                    request.getAdvertisedHost(),
                    request.getAdvertisedPort(),
                    System.currentTimeMillis());
            responseObserver.onNext(
                    BrokerRegistrationResponse.newBuilder()
                            .setAccepted(true)
                            .setClusterId(clusterId)
                            .build());
            responseObserver.onCompleted();
        } catch (RuntimeException exception) {
            log.error("Failed to register broker via metadata command service", exception);
            responseObserver.onError(exception);
        }
    }

    @Override
    public void watchReplicaAssignments(
            ReplicaAssignmentWatchRequest request,
            StreamObserver<ReplicaAssignmentEvent> responseObserver) {
        int brokerId = request.getBrokerId();
        ServerCallStreamObserver<ReplicaAssignmentEvent> serverObserver =
                (ServerCallStreamObserver<ReplicaAssignmentEvent>) responseObserver;
        Object writeLock = new Object();
        ControllerAssignmentRegistry.AssignmentListener listener =
                (version, assignments) -> {
                    synchronized (writeLock) {
                        if (serverObserver.isCancelled()) {
                            return;
                        }
                        responseObserver.onNext(
                                ReplicaAssignmentEvent.newBuilder()
                                        .setVersion(version)
                                        .addAllAssignments(assignments)
                                        .build());
                    }
                };
        long currentVersion = assignmentRegistry.addListener(brokerId, listener);
        serverObserver.setOnCancelHandler(() -> assignmentRegistry.removeListener(brokerId, listener));
        synchronized (writeLock) {
            if (!serverObserver.isCancelled()) {
                responseObserver.onNext(
                        ReplicaAssignmentEvent.newBuilder()
                                .setVersion(currentVersion)
                                .addAllAssignments(assignmentRegistry.assignments(brokerId))
                                .build());
            }
        }
    }

    @Override
    public void applyPartitionControl(
            PartitionControlBatchRequest request,
            StreamObserver<PartitionControlBatchResponse> responseObserver) {
        int applied = 0;
        for (PartitionControlCommandMessage command : request.getCommandsList()) {
            replicaCoordinator.apply(toCommand(command));
            applied++;
        }
        responseObserver.onNext(
                PartitionControlBatchResponse.newBuilder().setAppliedCount(applied).build());
        responseObserver.onCompleted();
    }

    @Override
    public void watchPartitionControlCommands(
            PartitionControlWatchRequest request,
            StreamObserver<PartitionControlCommandEvent> responseObserver) {
        int brokerId = request.getBrokerId();
        ServerCallStreamObserver<PartitionControlCommandEvent> serverObserver =
                (ServerCallStreamObserver<PartitionControlCommandEvent>) responseObserver;
        Object writeLock = new Object();
        ControllerPartitionControlRegistry.PartitionCommandListener listener =
                (version, commands) -> {
                    synchronized (writeLock) {
                        if (serverObserver.isCancelled()) {
                            return;
                        }
                        responseObserver.onNext(
                                PartitionControlCommandEvent.newBuilder()
                                        .setVersion(version)
                                        .addAllCommands(commands)
                                        .build());
                    }
                };
        long currentVersion = partitionControlRegistry.addListener(brokerId, listener);
        serverObserver.setOnCancelHandler(
                () -> partitionControlRegistry.removeListener(brokerId, listener));
        synchronized (writeLock) {
            if (!serverObserver.isCancelled()) {
                responseObserver.onNext(
                        PartitionControlCommandEvent.newBuilder()
                                .setVersion(currentVersion)
                                .addAllCommands(partitionControlRegistry.commands(brokerId))
                                .build());
            }
        }
    }

    @Override
    public void reportPartitionControlResults(
            PartitionControlReportRequest request,
            StreamObserver<PartitionControlReportResponse> responseObserver) {
        metadataStateMachine.recordPartitionControlResults(
                request.getBrokerId(), request.getResultsList());
        int successCount = 0;
        for (PartitionControlApplyResult result : request.getResultsList()) {
            if (result.getSuccess()) {
                successCount++;
            }
        }
        responseObserver.onNext(
                PartitionControlReportResponse.newBuilder().setAcceptedCount(successCount).build());
        responseObserver.onCompleted();
    }

    private PartitionControlCommand toCommand(PartitionControlCommandMessage command) {
        return new PartitionControlCommand(
                command.getTopic(),
                command.getPartition(),
                command.getLeaderId(),
                command.getLeaderEpoch(),
                new ArrayList<>(command.getReplicaNodesList()),
                new ArrayList<>(command.getIsrNodesList()),
                command.getHasTruncateToLeaderEpoch() ? command.getTruncateToLeaderEpoch() : null,
                command.getHasTruncateToOffset() ? command.getTruncateToOffset() : null);
    }
}
