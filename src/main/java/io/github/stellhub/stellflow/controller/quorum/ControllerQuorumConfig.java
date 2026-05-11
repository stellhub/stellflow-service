package io.github.stellhub.stellflow.controller.quorum;

import io.github.stellhub.stellflow.config.EndpointParser;
import io.github.stellhub.stellflow.config.StellflowConfigLoader;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;

/**
 * Controller quorum 配置。
 */
@Builder
@Getter
public class ControllerQuorumConfig {

    private static final String PREFIX = "stellflow.controller.quorum.";

    @Builder.Default private final boolean enabled = false;
    @Builder.Default private final String selfId = "c1";
    @Builder.Default
    private final String groupId = "11111111-1111-1111-1111-111111111111";
    @Builder.Default private final String endpoint = "grpc://127.0.0.1:19195";
    @Builder.Default private final Path storageDir = Path.of("data/controller-quorum");
    @Builder.Default private final String peers = "c1@grpc://127.0.0.1:19195";
    @Builder.Default private final int requestTimeoutMs = 3000;

    /**
     * 从统一配置加载 quorum 配置。
     */
    public static ControllerQuorumConfig load() {
        ControllerQuorumConfig defaults = ControllerQuorumConfig.builder().build();
        Properties properties = StellflowConfigLoader.load();
        String endpoint = readString(properties, "endpoint", defaults.getEndpoint());
        EndpointParser.parse(endpoint, "grpc");
        return ControllerQuorumConfig.builder()
                .enabled(readBoolean(properties, "enabled", defaults.isEnabled()))
                .selfId(readString(properties, "selfId", defaults.getSelfId()))
                .groupId(readString(properties, "groupId", defaults.getGroupId()))
                .endpoint(endpoint)
                .storageDir(
                        Path.of(
                                readString(
                                        properties,
                                        "storageDir",
                                        defaults.getStorageDir().toString())))
                .peers(readString(properties, "peers", defaults.getPeers()))
                .requestTimeoutMs(
                        readPositiveInt(
                                properties, "requestTimeoutMs", defaults.getRequestTimeoutMs()))
                .build();
    }

    /**
     * 解析 quorum peer 列表。
     */
    public List<ControllerQuorumPeer> parsedPeers() {
        return Arrays.stream(peers.split(","))
                .map(String::trim)
                .filter(value -> !value.isBlank())
                .map(ControllerQuorumPeer::parse)
                .collect(Collectors.toUnmodifiableList());
    }

    /**
     * 解析自身 endpoint。
     */
    public EndpointParser.ParsedEndpoint parsedSelfEndpoint() {
        return EndpointParser.parse(endpoint, "grpc");
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
