package io.github.stellhub.stellflow;

import io.github.stellhub.stellflow.network.protocol.ProtocolCodecRegistry;
import io.github.stellhub.stellflow.network.transport.NettyTransportConfig;
import io.github.stellhub.stellflow.network.transport.SocketServer;
import io.github.stellhub.stellflow.controller.control.ControlPlaneGrpcConfig;
import io.github.stellhub.stellflow.controller.control.ControllerAssignmentRegistry;
import io.github.stellhub.stellflow.controller.control.ControllerBrokerControlClient;
import io.github.stellhub.stellflow.controller.control.ControllerBrokerControlServer;
import io.github.stellhub.stellflow.controller.control.ControllerPartitionControlRegistry;
import io.github.stellhub.stellflow.controller.control.PartitionControlResultRegistry;
import io.github.stellhub.stellflow.controller.replica.ReplicaFetchConfig;
import io.github.stellhub.stellflow.controller.replica.ReplicaFetchManager;
import io.github.stellhub.stellflow.observability.metrics.MetricsHttpConfig;
import io.github.stellhub.stellflow.observability.metrics.PrometheusMetricsHttpServer;
import io.github.stellhub.stellflow.observability.metrics.ReplicaFetchMetrics;
import io.github.stellhub.stellflow.server.api.BrokerApis;
import io.github.stellhub.stellflow.server.api.InMemoryRequestChannel;
import io.github.stellhub.stellflow.server.api.RequestChannel;
import io.github.stellhub.stellflow.server.api.RequestDispatcher;
import io.github.stellhub.stellflow.server.api.ResponseResponder;
import lombok.extern.slf4j.Slf4j;

/**
 * Broker 启动入口骨架。
 */
@Slf4j
public final class StellflowBrokerApplication {

    private StellflowBrokerApplication() {}

    /**
     * 启动 Broker 最小骨架。
     */
    public static void main(String[] args) throws Exception {
        log.info("Starting Stellflow broker skeleton");
        RequestChannel requestChannel = new InMemoryRequestChannel();
        ProtocolCodecRegistry protocolCodecRegistry = ProtocolCodecRegistry.defaultRegistry();
        NettyTransportConfig transportConfig = NettyTransportConfig.load();
        BrokerApis brokerApis =
                BrokerApis.defaultBrokerApis(transportConfig.getHost(), transportConfig.getPort());
        ReplicaFetchMetrics replicaFetchMetrics = new ReplicaFetchMetrics();
        ReplicaFetchConfig replicaFetchConfig = ReplicaFetchConfig.load();
        ReplicaFetchManager replicaFetchManager =
                ReplicaFetchManager.fromConfig(
                        replicaFetchConfig, brokerApis.logManager(), replicaFetchMetrics);
        ControlPlaneGrpcConfig controlPlaneGrpcConfig = ControlPlaneGrpcConfig.load();
        ControllerBrokerControlServer controlServer =
                new ControllerBrokerControlServer(
                        controlPlaneGrpcConfig,
                        new ControllerAssignmentRegistry(),
                        new ControllerPartitionControlRegistry(),
                        new PartitionControlResultRegistry(),
                        brokerApis.controllerReplicaCoordinator());
        ControllerBrokerControlClient controlClient =
                new ControllerBrokerControlClient(
                        controlPlaneGrpcConfig,
                        replicaFetchManager,
                        brokerApis.controllerReplicaCoordinator());
        PrometheusMetricsHttpServer metricsHttpServer =
                new PrometheusMetricsHttpServer(MetricsHttpConfig.load(), replicaFetchMetrics);

        RequestDispatcher requestDispatcher = new RequestDispatcher(requestChannel, brokerApis, 2);
        ResponseResponder responseResponder = new ResponseResponder(requestChannel);
        SocketServer socketServer =
                new SocketServer(transportConfig, protocolCodecRegistry, requestChannel);

        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    log.info("Shutdown hook triggered, closing broker components");
                                    socketServer.close();
                                    responseResponder.close();
                                    requestDispatcher.close();
                                    replicaFetchManager.close();
                                    controlClient.close();
                                    controlServer.close();
                                    metricsHttpServer.close();
                                    brokerApis.close();
                                    log.info("Broker components closed");
                                }));

        requestDispatcher.start();
        responseResponder.start();
        socketServer.start();
        controlServer.start();
        replicaFetchManager.start();
        controlClient.start();
        metricsHttpServer.start();
        log.info(
                "Stellflow broker skeleton started on {}:{}",
                transportConfig.getHost(),
                transportConfig.getPort());
        socketServer.awaitClose();
    }
}
