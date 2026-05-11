package io.github.stellhub.stellflow.controller.replica;

import io.github.stellhub.stellflow.config.EndpointParser;
import io.github.stellhub.stellflow.config.StellflowConfigLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import lombok.Builder;
import lombok.Getter;

/**
 * follower 后台拉取任务配置。
 */
@Builder
@Getter
public class ReplicaFetchConfig {

    private static final String PREFIX = "stellflow.replica.fetch.";

    @Builder.Default private final boolean enabled = false;
    @Builder.Default private final int followerBrokerId = 1;
    @Builder.Default private final int pollIntervalMs = 500;
    @Builder.Default private final int socketTimeoutMs = 3000;
    @Builder.Default private final int connectTimeoutMs = 3000;
    @Builder.Default private final int maxBytes = 4 * 1024 * 1024;
    @Builder.Default private final int maxWaitMs = 500;
    @Builder.Default private final int minBytes = 1;
    @Builder.Default private final int pipelineRoundsPerPoll = 4;
    @Builder.Default private final String assignments = "";

    /**
     * 从统一配置加载。
     */
    public static ReplicaFetchConfig load() {
        ReplicaFetchConfig defaults = ReplicaFetchConfig.builder().build();
        Properties properties = StellflowConfigLoader.load();
        return ReplicaFetchConfig.builder()
                .enabled(readBoolean(properties, "enabled", defaults.isEnabled()))
                .followerBrokerId(
                        readPositiveInt(
                                properties, "followerBrokerId", defaults.getFollowerBrokerId()))
                .pollIntervalMs(
                        readPositiveInt(properties, "pollIntervalMs", defaults.getPollIntervalMs()))
                .socketTimeoutMs(
                        readPositiveInt(
                                properties, "socketTimeoutMs", defaults.getSocketTimeoutMs()))
                .connectTimeoutMs(
                        readPositiveInt(
                                properties, "connectTimeoutMs", defaults.getConnectTimeoutMs()))
                .maxBytes(readPositiveInt(properties, "maxBytes", defaults.getMaxBytes()))
                .maxWaitMs(readPositiveInt(properties, "maxWaitMs", defaults.getMaxWaitMs()))
                .minBytes(readPositiveInt(properties, "minBytes", defaults.getMinBytes()))
                .pipelineRoundsPerPoll(
                        readPositiveInt(
                                properties,
                                "pipelineRoundsPerPoll",
                                defaults.getPipelineRoundsPerPoll()))
                .assignments(readString(properties, "assignments", defaults.getAssignments()))
                .build();
    }

    /**
     * 解析 assignments 配置。
     */
    public List<ReplicaFetchAssignment> parseAssignments() {
        if (assignments == null || assignments.isBlank()) {
            return List.of();
        }
        List<ReplicaFetchAssignment> parsed = new ArrayList<>();
        String[] rawAssignments = assignments.split(",");
        for (String rawAssignment : rawAssignments) {
            String trimmed = rawAssignment.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            parsed.add(parseAssignment(trimmed));
        }
        return List.copyOf(parsed);
    }

    private ReplicaFetchAssignment parseAssignment(String value) {
        String[] topicAndEndpoint = value.split("@", 2);
        if (topicAndEndpoint.length != 2) {
            throw new IllegalArgumentException(
                    "Invalid replica assignment, expected topic:partition@host:port#leaderBrokerId, but was "
                            + value);
        }
        String[] topicAndPartition = topicAndEndpoint[0].split(":", 2);
        if (topicAndPartition.length != 2) {
            throw new IllegalArgumentException("Invalid topic-partition assignment: " + value);
        }
        String[] endpointAndLeader = topicAndEndpoint[1].split("#", 2);
        if (endpointAndLeader.length != 2) {
            throw new IllegalArgumentException("Invalid endpoint/leader assignment: " + value);
        }
        EndpointParser.ParsedEndpoint parsedEndpoint =
                EndpointParser.parseLenient(endpointAndLeader[0], "stellflow", "stellflow");
        return new ReplicaFetchAssignment(
                topicAndPartition[0],
                Integer.parseInt(topicAndPartition[1]),
                parsedEndpoint.host(),
                parsedEndpoint.port(),
                Integer.parseInt(endpointAndLeader[1]));
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
