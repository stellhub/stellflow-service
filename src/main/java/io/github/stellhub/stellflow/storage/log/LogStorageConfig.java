package io.github.stellhub.stellflow.storage.log;

import io.github.stellhub.stellflow.config.StellflowConfigLoader;
import java.nio.file.Path;
import java.util.Properties;
import lombok.Builder;
import lombok.Getter;

/**
 * storage.log 配置。
 */
@Builder
@Getter
public class LogStorageConfig {

    private static final String ROOT_DIR_KEY = "stellflow.storage.log.rootDir";
    private static final String SEGMENT_BYTES_KEY = "stellflow.storage.log.segmentBytes";
    private static final String INDEX_INTERVAL_BYTES_KEY =
            "stellflow.storage.log.indexIntervalBytes";
    private static final String RETENTION_SEGMENTS_KEY = "stellflow.storage.log.retentionSegments";
    private static final String RETENTION_MS_KEY = "stellflow.storage.log.retentionMs";
    private static final String RETENTION_BYTES_KEY = "stellflow.storage.log.retentionBytes";
    private static final String FLUSH_EVERY_APPEND_KEY = "stellflow.storage.log.flushEveryAppend";

    @Builder.Default private final Path rootDir = Path.of("data", "logs");
    @Builder.Default private final int segmentBytes = 32 * 1024 * 1024;
    @Builder.Default private final int indexIntervalBytes = 4 * 1024;
    @Builder.Default private final int retentionSegments = 8;
    @Builder.Default private final long retentionMs = 7L * 24 * 60 * 60 * 1000;
    @Builder.Default private final long retentionBytes = 1024L * 1024 * 1024;
    @Builder.Default private final boolean flushEveryAppend = true;

    /**
     * 从统一 YAML 配置加载。
     */
    public static LogStorageConfig load() {
        LogStorageConfig defaults = LogStorageConfig.builder().build();
        Properties properties = StellflowConfigLoader.load();
        String rootDir =
                StellflowConfigLoader.readString(
                        properties, ROOT_DIR_KEY, defaults.getRootDir().toString());
        int segmentBytes =
                StellflowConfigLoader.readPositiveInt(
                        properties, SEGMENT_BYTES_KEY, defaults.getSegmentBytes());
        int indexIntervalBytes =
                StellflowConfigLoader.readPositiveInt(
                        properties,
                        INDEX_INTERVAL_BYTES_KEY,
                        defaults.getIndexIntervalBytes());
        int retentionSegments =
                StellflowConfigLoader.readPositiveInt(
                        properties, RETENTION_SEGMENTS_KEY, defaults.getRetentionSegments());
        long retentionMs =
                StellflowConfigLoader.readPositiveLong(
                        properties, RETENTION_MS_KEY, defaults.getRetentionMs());
        long retentionBytes =
                StellflowConfigLoader.readPositiveLong(
                        properties, RETENTION_BYTES_KEY, defaults.getRetentionBytes());
        boolean flushEveryAppend =
                StellflowConfigLoader.readBoolean(
                        properties, FLUSH_EVERY_APPEND_KEY, defaults.isFlushEveryAppend());
        return LogStorageConfig.builder()
                .rootDir(Path.of(rootDir))
                .segmentBytes(segmentBytes)
                .indexIntervalBytes(indexIntervalBytes)
                .retentionSegments(retentionSegments)
                .retentionMs(retentionMs)
                .retentionBytes(retentionBytes)
                .flushEveryAppend(flushEveryAppend)
                .build();
    }
}
