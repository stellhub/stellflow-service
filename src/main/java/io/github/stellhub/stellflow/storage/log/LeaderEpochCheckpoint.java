package io.github.stellhub.stellflow.storage.log;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * leader epoch 到起始 offset 的检查点文件。
 */
public class LeaderEpochCheckpoint {

    private final Path checkpointFile;
    private final NavigableMap<Integer, Long> epochOffsets = new TreeMap<>();

    public LeaderEpochCheckpoint(Path checkpointFile) {
        this.checkpointFile = checkpointFile;
        load();
    }

    /**
     * 为 epoch 注册起始 offset。
     */
    public synchronized void assignIfAbsent(int epoch, long startOffset) {
        if (epoch < 0) {
            return;
        }
        epochOffsets.putIfAbsent(epoch, startOffset);
        persist();
    }

    /**
     * 重建完整 epoch 映射。
     */
    public synchronized void rebuild(NavigableMap<Integer, Long> source) {
        epochOffsets.clear();
        epochOffsets.putAll(source);
        persist();
    }

    /**
     * 返回指定 epoch 之后下一个 epoch 的起始 offset。
     */
    public synchronized Long nextEpochStartOffset(int epoch) {
        Map.Entry<Integer, Long> next = epochOffsets.higherEntry(epoch);
        return next == null ? null : next.getValue();
    }

    /**
     * 返回当前快照。
     */
    public synchronized NavigableMap<Integer, Long> snapshot() {
        return new TreeMap<>(epochOffsets);
    }

    private void load() {
        epochOffsets.clear();
        if (!Files.exists(checkpointFile)) {
            return;
        }
        try {
            for (String line : Files.readAllLines(checkpointFile, StandardCharsets.UTF_8)) {
                String trimmed = line.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                String[] parts = trimmed.split("\\s+");
                if (parts.length != 2) {
                    continue;
                }
                epochOffsets.put(Integer.parseInt(parts[0]), Long.parseLong(parts[1]));
            }
        } catch (IOException exception) {
            throw new IllegalStateException(
                    "Failed to load leader epoch checkpoint " + checkpointFile, exception);
        }
    }

    private void persist() {
        try {
            Files.createDirectories(checkpointFile.getParent());
            StringBuilder builder = new StringBuilder();
            for (Map.Entry<Integer, Long> entry : epochOffsets.entrySet()) {
                builder.append(entry.getKey()).append(' ').append(entry.getValue()).append('\n');
            }
            Files.writeString(checkpointFile, builder.toString(), StandardCharsets.UTF_8);
        } catch (IOException exception) {
            throw new IllegalStateException(
                    "Failed to persist leader epoch checkpoint " + checkpointFile, exception);
        }
    }
}
