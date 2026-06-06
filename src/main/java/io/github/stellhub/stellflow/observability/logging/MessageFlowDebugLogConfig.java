package io.github.stellhub.stellflow.observability.logging;

import io.github.stellhub.stellflow.config.StellflowConfigLoader;
import java.util.Properties;
import lombok.Builder;
import lombok.Getter;

/**
 * 消息流调试日志配置。
 */
@Builder
@Getter
public class MessageFlowDebugLogConfig {

    private static final String PREFIX = "stellflow.observability.logging.messageFlowDebug.";

    @Builder.Default private final boolean enabled = false;

    /**
     * 从配置文件加载消息流调试日志配置。
     */
    public static MessageFlowDebugLogConfig load() {
        MessageFlowDebugLogConfig defaults = MessageFlowDebugLogConfig.builder().build();
        Properties properties = StellflowConfigLoader.load();
        return MessageFlowDebugLogConfig.builder()
                .enabled(readBoolean(properties, "enabled", defaults.isEnabled()))
                .build();
    }

    private static boolean readBoolean(Properties properties, String key, boolean defaultValue) {
        return StellflowConfigLoader.readBoolean(properties, PREFIX + key, defaultValue);
    }
}
