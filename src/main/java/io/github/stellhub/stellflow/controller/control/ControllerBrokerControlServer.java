package io.github.stellhub.stellflow.controller.control;

import io.github.stellhub.stellflow.controller.quorum.ControllerQuorumConfig;
import io.github.stellhub.stellflow.controller.quorum.ControllerQuorumManager;
import io.github.stellhub.stellflow.controller.replica.ControllerReplicaCoordinator;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;

/**
 * Controller/Broker gRPC 服务端。
 */
@Slf4j
public class ControllerBrokerControlServer implements AutoCloseable {

    private final ControlPlaneGrpcConfig config;
    private final ControllerAssignmentRegistry assignmentRegistry;
    private final ControllerPartitionControlRegistry partitionControlRegistry;
    private final PartitionControlResultRegistry partitionControlResultRegistry;
    private final ControllerMetadataStateMachine metadataStateMachine;
    private final ControllerMetadataCommandService metadataCommandService;
    private final ControllerMetadataDecisionService metadataDecisionService;
    private final ControllerMetadataAutoReconciler metadataAutoReconciler;
    private final ControllerQuorumManager quorumManager;
    private final ControllerReplicaCoordinator replicaCoordinator;
    private Server server;

    public ControllerBrokerControlServer(
            ControlPlaneGrpcConfig config,
            ControllerAssignmentRegistry assignmentRegistry,
            ControllerPartitionControlRegistry partitionControlRegistry,
            PartitionControlResultRegistry partitionControlResultRegistry,
            ControllerReplicaCoordinator replicaCoordinator) {
        this(
                config,
                assignmentRegistry,
                partitionControlRegistry,
                partitionControlResultRegistry,
                ControllerQuorumConfig.load(),
                replicaCoordinator);
    }

    public ControllerBrokerControlServer(
            ControlPlaneGrpcConfig config,
            ControllerAssignmentRegistry assignmentRegistry,
            ControllerPartitionControlRegistry partitionControlRegistry,
            PartitionControlResultRegistry partitionControlResultRegistry,
            ControllerQuorumConfig quorumConfig,
            ControllerReplicaCoordinator replicaCoordinator) {
        this.config = config;
        this.assignmentRegistry = assignmentRegistry;
        this.partitionControlRegistry = partitionControlRegistry;
        this.partitionControlResultRegistry = partitionControlResultRegistry;
        this.metadataStateMachine =
                new ControllerMetadataStateMachine(
                        assignmentRegistry, partitionControlRegistry, partitionControlResultRegistry);
        this.quorumManager =
                quorumConfig.isEnabled()
                        ? new ControllerQuorumManager(quorumConfig, metadataStateMachine)
                        : null;
        this.metadataCommandService =
                quorumManager != null
                        ? quorumManager
                        : new DirectControllerMetadataCommandService(metadataStateMachine);
        this.metadataDecisionService =
                new ControllerMetadataDecisionService(
                        metadataCommandService, metadataStateMachine);
        this.metadataAutoReconciler =
                new ControllerMetadataAutoReconciler(
                        ControllerAutoReconcileConfig.load(),
                        metadataStateMachine,
                        metadataDecisionService,
                        replicaCoordinator);
        this.replicaCoordinator = replicaCoordinator;
    }

    /**
     * 启动 gRPC 控制面服务端。
     */
    public synchronized void start() {
        if (!config.isServerEnabled() || server != null) {
            return;
        }
        try {
            if (quorumManager != null) {
                quorumManager.start();
            }
            server =
                    NettyServerBuilder.forAddress(
                                    new java.net.InetSocketAddress(
                                            config.getServerHost(), config.getServerPort()))
                            .addService(
                                    new ControllerBrokerControlServiceImpl(
                                            assignmentRegistry,
                                            partitionControlRegistry,
                                            partitionControlResultRegistry,
                                            metadataCommandService,
                                            metadataStateMachine,
                                            replicaCoordinator,
                                            config.getClusterId()))
                            .build()
                            .start();
            metadataAutoReconciler.start();
            log.info(
                    "Controller/Broker gRPC server started on {}:{}",
                    config.getServerHost(),
                    config.getServerPort());
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to start control plane gRPC server", exception);
        }
    }

    public ControllerAssignmentRegistry assignmentRegistry() {
        return assignmentRegistry;
    }

    public ControllerPartitionControlRegistry partitionControlRegistry() {
        return partitionControlRegistry;
    }

    public PartitionControlResultRegistry partitionControlResultRegistry() {
        return partitionControlResultRegistry;
    }

    public ControllerMetadataStateMachine metadataStateMachine() {
        return metadataStateMachine;
    }

    public ControllerMetadataCommandService metadataCommandService() {
        return metadataCommandService;
    }

    public ControllerMetadataDecisionService metadataDecisionService() {
        return metadataDecisionService;
    }

    @Override
    public synchronized void close() {
        if (server == null) {
            metadataAutoReconciler.close();
            if (quorumManager != null) {
                quorumManager.close();
            }
            return;
        }
        server.shutdownNow();
        server = null;
        metadataAutoReconciler.close();
        if (quorumManager != null) {
            quorumManager.close();
        }
        log.info("Controller/Broker gRPC server stopped");
    }
}
