package io.github.stellhub.stellflow.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import org.yaml.snakeyaml.Yaml;

/**
 * Stellflow YAML 配置加载器。
 */
public final class StellflowConfigLoader {

    public static final String CONFIG_FILE_PROPERTY = "stellflow.config.file";
    private static final String DEFAULT_CONFIG_FILE = "stellflow.yaml";

    private StellflowConfigLoader() {}

    /**
     * 加载并展平配置。
     */
    public static Properties load() {
        Properties properties = new Properties();
        loadClasspathYaml(properties);
        loadExternalYaml(properties);
        return properties;
    }

    /**
     * 读取字符串配置，系统属性优先。
     */
    public static String readString(Properties properties, String key, String defaultValue) {
        String systemValue = System.getProperty(key);
        if (systemValue != null && !systemValue.isBlank()) {
            return systemValue;
        }
        String value = properties.getProperty(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value.trim();
    }

    /**
     * 读取正整数配置。
     */
    public static int readPositiveInt(Properties properties, String key, int defaultValue) {
        String rawValue = readString(properties, key, Integer.toString(defaultValue));
        int value = Integer.parseInt(rawValue);
        if (value <= 0) {
            throw new IllegalArgumentException(key + " must be positive, but was " + value);
        }
        return value;
    }

    /**
     * 读取正 long 配置。
     */
    public static long readPositiveLong(Properties properties, String key, long defaultValue) {
        String rawValue = readString(properties, key, Long.toString(defaultValue));
        long value = Long.parseLong(rawValue);
        if (value <= 0) {
            throw new IllegalArgumentException(key + " must be positive, but was " + value);
        }
        return value;
    }

    /**
     * 读取非负 long 配置。
     */
    public static long readNonNegativeLong(Properties properties, String key, long defaultValue) {
        String rawValue = readString(properties, key, Long.toString(defaultValue));
        long value = Long.parseLong(rawValue);
        if (value < 0) {
            throw new IllegalArgumentException(key + " must be non-negative, but was " + value);
        }
        return value;
    }

    /**
     * 读取布尔配置。
     */
    public static boolean readBoolean(Properties properties, String key, boolean defaultValue) {
        String rawValue = readString(properties, key, Boolean.toString(defaultValue));
        if ("true".equalsIgnoreCase(rawValue)) {
            return true;
        }
        if ("false".equalsIgnoreCase(rawValue)) {
            return false;
        }
        throw new IllegalArgumentException(key + " must be true or false, but was " + rawValue);
    }

    /**
     * 从类路径加载默认 YAML。
     */
    private static void loadClasspathYaml(Properties properties) {
        try (InputStream inputStream =
                StellflowConfigLoader.class.getClassLoader().getResourceAsStream(DEFAULT_CONFIG_FILE)) {
            if (inputStream != null) {
                loadYamlIntoProperties(inputStream, properties);
            }
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to load classpath stellflow.yaml", exception);
        }
    }

    /**
     * 从外部 YAML 覆盖配置。
     */
    private static void loadExternalYaml(Properties properties) {
        String configFile = System.getProperty(CONFIG_FILE_PROPERTY);
        if (configFile == null || configFile.isBlank()) {
            return;
        }
        Path path = Path.of(configFile);
        try (InputStream inputStream = Files.newInputStream(path)) {
            loadYamlIntoProperties(inputStream, properties);
        } catch (IOException exception) {
            throw new IllegalStateException(
                    "Failed to load external config file: " + path.toAbsolutePath(), exception);
        }
    }

    /**
     * 将 YAML 结构展平成点分隔键。
     */
    @SuppressWarnings("unchecked")
    private static void loadYamlIntoProperties(InputStream inputStream, Properties properties) {
        Object loaded = new Yaml().load(inputStream);
        if (!(loaded instanceof Map<?, ?> root)) {
            return;
        }
        flatten("", (Map<String, Object>) root, properties);
    }

    /**
     * 递归展平嵌套 map。
     */
    @SuppressWarnings("unchecked")
    private static void flatten(String prefix, Map<String, Object> source, Properties properties) {
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String key = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Map<?, ?> nested) {
                flatten(key, (Map<String, Object>) nested, properties);
                continue;
            }
            if (value != null) {
                properties.setProperty(key, String.valueOf(value));
            }
        }
    }
}
