package io.github.stellhub.stellflow.storage.log;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Properties;

/**
 * 分区 checkpoint 持久化。
 */
public class LogCheckpoint {

    private static final String HIGH_WATERMARK_KEY = "highWatermark";
    private static final String LOG_END_OFFSET_KEY = "logEndOffset";
    private static final String RECOVERY_POINT_KEY = "recoveryPoint";
    private static final String LOG_START_OFFSET_KEY = "logStartOffset";
    private static final String LEADER_EPOCH_KEY = "leaderEpoch";

    private final Path checkpointFile;

    public LogCheckpoint(Path checkpointFile) {
        this.checkpointFile = checkpointFile;
    }

    /**
     * 加载 checkpoint 状态。
     */
    public LogCheckpointState load() {
        if (!Files.exists(checkpointFile)) {
            return new LogCheckpointState(0, 0, 0, 0, 0);
        }
        Properties properties = new Properties();
        try (InputStream inputStream = Files.newInputStream(checkpointFile)) {
            properties.load(inputStream);
        } catch (IOException exception) {
            throw new IllegalStateException(
                    "Failed to load checkpoint file " + checkpointFile, exception);
        }
        return new LogCheckpointState(
                Long.parseLong(properties.getProperty(HIGH_WATERMARK_KEY, "0")),
                Long.parseLong(properties.getProperty(LOG_END_OFFSET_KEY, "0")),
                Long.parseLong(properties.getProperty(RECOVERY_POINT_KEY, "0")),
                Long.parseLong(properties.getProperty(LOG_START_OFFSET_KEY, "0")),
                Integer.parseInt(properties.getProperty(LEADER_EPOCH_KEY, "0")));
    }

    /**
     * 持久化 checkpoint 状态。
     */
    public synchronized void persist(LogCheckpointState state) {
        try {
            Files.createDirectories(checkpointFile.getParent());
            Path tempFile = checkpointFile.resolveSibling(checkpointFile.getFileName() + ".tmp");
            Properties properties = new Properties();
            properties.setProperty(HIGH_WATERMARK_KEY, Long.toString(state.highWatermark()));
            properties.setProperty(LOG_END_OFFSET_KEY, Long.toString(state.logEndOffset()));
            properties.setProperty(RECOVERY_POINT_KEY, Long.toString(state.recoveryPoint()));
            properties.setProperty(LOG_START_OFFSET_KEY, Long.toString(state.logStartOffset()));
            properties.setProperty(LEADER_EPOCH_KEY, Integer.toString(state.leaderEpoch()));
            try (OutputStream outputStream = Files.newOutputStream(tempFile)) {
                properties.store(outputStream, "stellflow log checkpoint");
            }
            Files.move(
                    tempFile,
                    checkpointFile,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException exception) {
            throw new IllegalStateException(
                    "Failed to persist checkpoint file " + checkpointFile, exception);
        }
    }
}
