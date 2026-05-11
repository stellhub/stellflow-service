package io.github.stellhub.stellflow.controller.control;

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
    private final ControllerReplicaCoordinator replicaCoordinator;
    private Server server;

    public ControllerBrokerControlServer(
            ControlPlaneGrpcConfig config,
            ControllerAssignmentRegistry assignmentRegistry,
            ControllerPartitionControlRegistry partitionControlRegistry,
            PartitionControlResultRegistry partitionControlResultRegistry,
            ControllerReplicaCoordinator replicaCoordinator) {
        this.config = config;
        this.assignmentRegistry = assignmentRegistry;
        this.partitionControlRegistry = partitionControlRegistry;
        this.partitionControlResultRegistry = partitionControlResultRegistry;
        this.metadataStateMachine =
                new ControllerMetadataStateMachine(
                        assignmentRegistry, partitionControlRegistry, partitionControlResultRegistry);
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
            server =
                    NettyServerBuilder.forAddress(
                                    new java.net.InetSocketAddress(
                                            config.getServerHost(), config.getServerPort()))
                            .addService(
                                    new ControllerBrokerControlServiceImpl(
                                            assignmentRegistry,
                                            partitionControlRegistry,
                                            partitionControlResultRegistry,
                                            metadataStateMachine,
                                            replicaCoordinator,
                                            config.getClusterId()))
                            .build()
                            .start();
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

    @Override
    public synchronized void close() {
        if (server == null) {
            return;
        }
        server.shutdownNow();
        server = null;
        log.info("Controller/Broker gRPC server stopped");
    }
}
