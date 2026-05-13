package io.github.stellhub.stellflow.controller.control;

import io.github.stellhub.stellflow.config.StellflowConfigLoader;
import java.util.Properties;
import lombok.Builder;
import lombok.Getter;

/**
 * Controller 自动决策与收敛配置。
 */
@Builder
@Getter
public class ControllerAutoReconcileConfig {

    private static final String PREFIX = "stellflow.controlPlane.reconcile.";
    private static final String LEGACY_PREFIX = "stellflow.controller.reconcile.";

    @Builder.Default private final boolean enabled = true;
    @Builder.Default private final int intervalMs = 1000;
    @Builder.Default private final long brokerHeartbeatTimeoutMs = 5000;
    @Builder.Default private final long maxReplicaLagMessages = 0;
    @Builder.Default private final boolean uncleanLeaderElectionEnabled = false;

    /**
     * 从统一 YAML 配置加载。
     */
    public static ControllerAutoReconcileConfig load() {
        ControllerAutoReconcileConfig defaults = ControllerAutoReconcileConfig.builder().build();
        Properties properties = StellflowConfigLoader.load();
        return ControllerAutoReconcileConfig.builder()
                .enabled(
                        readBoolean(properties, "enabled", defaults.isEnabled()))
                .intervalMs(
                        readPositiveInt(properties, "intervalMs", defaults.getIntervalMs()))
                .brokerHeartbeatTimeoutMs(
                        readPositiveLong(
                                properties,
                                "brokerHeartbeatTimeoutMs",
                                defaults.getBrokerHeartbeatTimeoutMs()))
                .maxReplicaLagMessages(
                        readNonNegativeLong(
                                properties,
                                "maxReplicaLagMessages",
                                defaults.getMaxReplicaLagMessages()))
                .uncleanLeaderElectionEnabled(
                        readBoolean(
                                properties,
                                "uncleanLeaderElectionEnabled",
                                defaults.isUncleanLeaderElectionEnabled()))
                .build();
    }

    private static boolean readBoolean(Properties properties, String key, boolean defaultValue) {
        return StellflowConfigLoader.readBoolean(
                properties, effectiveKey(properties, key), defaultValue);
    }

    private static int readPositiveInt(Properties properties, String key, int defaultValue) {
        return StellflowConfigLoader.readPositiveInt(
                properties, effectiveKey(properties, key), defaultValue);
    }

    private static long readPositiveLong(Properties properties, String key, long defaultValue) {
        return StellflowConfigLoader.readPositiveLong(
                properties, effectiveKey(properties, key), defaultValue);
    }

    private static long readNonNegativeLong(Properties properties, String key, long defaultValue) {
        return StellflowConfigLoader.readNonNegativeLong(
                properties, effectiveKey(properties, key), defaultValue);
    }

    private static String effectiveKey(Properties properties, String key) {
        String primaryKey = PREFIX + key;
        if (System.getProperty(primaryKey) != null || properties.containsKey(primaryKey)) {
            return primaryKey;
        }
        return LEGACY_PREFIX + key;
    }
}
