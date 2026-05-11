package io.github.stellhub.stellflow.observability.metrics;

import io.github.stellhub.stellflow.config.StellflowConfigLoader;
import java.util.Properties;
import lombok.Builder;
import lombok.Getter;

/**
 * Prometheus 拉取端口配置。
 */
@Builder
@Getter
public class MetricsHttpConfig {

    private static final String PREFIX = "stellflow.observability.metrics.http.";

    @Builder.Default private final boolean enabled = true;
    @Builder.Default private final String host = "0.0.0.0";
    @Builder.Default private final int port = 9464;
    @Builder.Default private final String path = "/metrics";

    /**
     * 从统一配置加载。
     */
    public static MetricsHttpConfig load() {
        MetricsHttpConfig defaults = MetricsHttpConfig.builder().build();
        Properties properties = StellflowConfigLoader.load();
        return MetricsHttpConfig.builder()
                .enabled(readBoolean(properties, "enabled", defaults.isEnabled()))
                .host(readString(properties, "host", defaults.getHost()))
                .port(readPositiveInt(properties, "port", defaults.getPort()))
                .path(readString(properties, "path", defaults.getPath()))
                .build();
    }

    /**
     * 读取字符串配置。
     */
    private static String readString(Properties properties, String key, String defaultValue) {
        return StellflowConfigLoader.readString(properties, PREFIX + key, defaultValue);
    }

    /**
     * 读取正整数配置。
     */
    private static int readPositiveInt(Properties properties, String key, int defaultValue) {
        return StellflowConfigLoader.readPositiveInt(properties, PREFIX + key, defaultValue);
    }

    /**
     * 读取布尔配置。
     */
    private static boolean readBoolean(Properties properties, String key, boolean defaultValue) {
        return StellflowConfigLoader.readBoolean(properties, PREFIX + key, defaultValue);
    }
}
