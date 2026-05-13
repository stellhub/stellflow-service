package io.github.stellhub.stellflow.controller.control;

import io.github.stellhub.stellflow.config.EndpointParser;
import io.github.stellhub.stellflow.config.StellflowConfigLoader;
import java.util.Properties;
import lombok.Builder;
import lombok.Getter;

/**
 * Controller/Broker gRPC 控制面配置。
 */
@Builder
@Getter
public class ControlPlaneGrpcConfig {

    private static final String PREFIX = "stellflow.controlPlane.grpc.";

    @Builder.Default private final boolean serverEnabled = false;
    @Builder.Default private final String serverHost = "0.0.0.0";
    @Builder.Default private final int serverPort = 19093;
    @Builder.Default private final boolean clientEnabled = false;
    @Builder.Default private final String controllerEndpoint = "grpc://127.0.0.1:19093";
    @Builder.Default private final String controllerHost = "127.0.0.1";
    @Builder.Default private final int controllerPort = 19093;
    @Builder.Default private final int brokerId = 1;
    @Builder.Default private final String advertisedEndpoint = "stellflow://127.0.0.1:9092";
    @Builder.Default private final String advertisedHost = "127.0.0.1";
    @Builder.Default private final int advertisedPort = 9092;
    @Builder.Default private final int watchReconnectBackoffMs = 1000;
    @Builder.Default private final int registrationIntervalMs = 1000;
    @Builder.Default private final String clusterId = "stellflow-dev-cluster";
    @Builder.Default private final boolean requirePersistentMetadata = false;

    /**
     * 从统一配置加载。
     */
    public static ControlPlaneGrpcConfig load() {
        ControlPlaneGrpcConfig defaults = ControlPlaneGrpcConfig.builder().build();
        Properties properties = StellflowConfigLoader.load();
        String controllerEndpoint =
                readString(properties, "controllerEndpoint", defaults.getControllerEndpoint());
        EndpointParser.ParsedEndpoint parsedControllerEndpoint =
                EndpointParser.parse(controllerEndpoint, "grpc");
        String advertisedEndpoint =
                readString(properties, "advertisedEndpoint", defaults.getAdvertisedEndpoint());
        EndpointParser.ParsedEndpoint parsedAdvertisedEndpoint =
                EndpointParser.parse(advertisedEndpoint, "stellflow");
        return ControlPlaneGrpcConfig.builder()
                .serverEnabled(readBoolean(properties, "serverEnabled", defaults.isServerEnabled()))
                .serverHost(readString(properties, "serverHost", defaults.getServerHost()))
                .serverPort(readPositiveInt(properties, "serverPort", defaults.getServerPort()))
                .clientEnabled(readBoolean(properties, "clientEnabled", defaults.isClientEnabled()))
                .controllerEndpoint(controllerEndpoint)
                .controllerHost(
                        readString(
                                properties,
                                "controllerHost",
                                parsedControllerEndpoint.host()))
                .controllerPort(
                        readPositiveInt(
                                properties,
                                "controllerPort",
                                parsedControllerEndpoint.port()))
                .brokerId(readPositiveInt(properties, "brokerId", defaults.getBrokerId()))
                .advertisedEndpoint(advertisedEndpoint)
                .advertisedHost(
                        readString(
                                properties,
                                "advertisedHost",
                                parsedAdvertisedEndpoint.host()))
                .advertisedPort(
                        readPositiveInt(
                                properties,
                                "advertisedPort",
                                parsedAdvertisedEndpoint.port()))
                .watchReconnectBackoffMs(
                        readPositiveInt(
                                properties,
                                "watchReconnectBackoffMs",
                                defaults.getWatchReconnectBackoffMs()))
                .registrationIntervalMs(
                        readPositiveInt(
                                properties,
                                "registrationIntervalMs",
                                defaults.getRegistrationIntervalMs()))
                .clusterId(readString(properties, "clusterId", defaults.getClusterId()))
                .requirePersistentMetadata(
                        readBoolean(
                                properties,
                                "requirePersistentMetadata",
                                defaults.isRequirePersistentMetadata()))
                .build();
    }

    private static String readString(Properties properties, String key, String defaultValue) {
        return StellflowConfigLoader.readString(properties, PREFIX + key, defaultValue);
    }

    private static boolean readBoolean(Properties properties, String key, boolean defaultValue) {
        return StellflowConfigLoader.readBoolean(properties, PREFIX + key, defaultValue);
    }

    private static int readPositiveInt(Properties properties, String key, int defaultValue) {
        return StellflowConfigLoader.readPositiveInt(properties, PREFIX + key, defaultValue);
    }
}
