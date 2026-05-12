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

    private static final String PREFIX = "stellflow.controller.reconcile.";

    @Builder.Default private final boolean enabled = true;
    @Builder.Default private final int intervalMs = 1000;
    @Builder.Default private final long brokerHeartbeatTimeoutMs = 5000;
    @Builder.Default private final long maxReplicaLagMessages = 0;

    /**
     * 从统一 YAML 配置加载。
     */
    public static ControllerAutoReconcileConfig load() {
        ControllerAutoReconcileConfig defaults = ControllerAutoReconcileConfig.builder().build();
        Properties properties = StellflowConfigLoader.load();
        return ControllerAutoReconcileConfig.builder()
                .enabled(
                        StellflowConfigLoader.readBoolean(
                                properties, PREFIX + "enabled", defaults.isEnabled()))
                .intervalMs(
                        StellflowConfigLoader.readPositiveInt(
                                properties, PREFIX + "intervalMs", defaults.getIntervalMs()))
                .brokerHeartbeatTimeoutMs(
                        StellflowConfigLoader.readPositiveLong(
                                properties,
                                PREFIX + "brokerHeartbeatTimeoutMs",
                                defaults.getBrokerHeartbeatTimeoutMs()))
                .maxReplicaLagMessages(
                        StellflowConfigLoader.readNonNegativeLong(
                                properties,
                                PREFIX + "maxReplicaLagMessages",
                                defaults.getMaxReplicaLagMessages()))
                .build();
    }
}
