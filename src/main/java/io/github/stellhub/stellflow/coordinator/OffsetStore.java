package io.github.stellhub.stellflow.coordinator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Offset 落盘存储。
 */
public class OffsetStore {

    private static final String DEFAULT_OFFSET_FILE = "data/consumer-offsets.snapshot";
    private final ConcurrentMap<OffsetKey, OffsetAndMetadata> offsets = new ConcurrentHashMap<>();
    private final Path snapshotFile;

    public OffsetStore() {
        this(Path.of(DEFAULT_OFFSET_FILE));
    }

    public OffsetStore(Path snapshotFile) {
        this.snapshotFile = snapshotFile;
        loadSnapshot();
    }

    /**
     * 提交消费位点。
     */
    public synchronized void commit(
            String groupId, String topic, int partition, long offset, String metadata) {
        offsets.put(
                new OffsetKey(groupId, topic, partition),
                new OffsetAndMetadata(offset, metadata, System.currentTimeMillis()));
        persistSnapshot();
    }

    /**
     * 查询消费位点。
     */
    public Optional<OffsetAndMetadata> fetch(String groupId, String topic, int partition) {
        return Optional.ofNullable(offsets.get(new OffsetKey(groupId, topic, partition)));
    }

    /**
     * 返回当前快照文件路径。
     */
    public Path snapshotFile() {
        return snapshotFile;
    }

    private void loadSnapshot() {
        if (!Files.exists(snapshotFile)) {
            return;
        }
        try {
            for (String line : Files.readAllLines(snapshotFile, StandardCharsets.UTF_8)) {
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }
                String[] parts = line.split("\\t", -1);
                if (parts.length != 6) {
                    continue;
                }
                OffsetKey key =
                        new OffsetKey(
                                decode(parts[0]), decode(parts[1]), Integer.parseInt(parts[2]));
                offsets.put(
                        key,
                        new OffsetAndMetadata(
                                Long.parseLong(parts[3]),
                                decode(parts[4]),
                                Long.parseLong(parts[5])));
            }
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to load offset snapshot " + snapshotFile, exception);
        }
    }

    private void persistSnapshot() {
        try {
            Files.createDirectories(snapshotFile.toAbsolutePath().getParent());
            Path tempFile = snapshotFile.resolveSibling(snapshotFile.getFileName() + ".tmp");
            StringBuilder builder = new StringBuilder();
            builder.append("# stellflow offset snapshot v1\n");
            offsets.entrySet().stream()
                    .sorted(
                            (left, right) -> {
                                int groupCompare =
                                        left.getKey().groupId().compareTo(right.getKey().groupId());
                                if (groupCompare != 0) {
                                    return groupCompare;
                                }
                                int topicCompare =
                                        left.getKey().topic().compareTo(right.getKey().topic());
                                if (topicCompare != 0) {
                                    return topicCompare;
                                }
                                return Integer.compare(
                                        left.getKey().partition(), right.getKey().partition());
                            })
                    .forEach(
                            entry ->
                                    builder.append(encode(entry.getKey().groupId()))
                                            .append('\t')
                                            .append(encode(entry.getKey().topic()))
                                            .append('\t')
                                            .append(entry.getKey().partition())
                                            .append('\t')
                                            .append(entry.getValue().offset())
                                            .append('\t')
                                            .append(encode(entry.getValue().metadata()))
                                            .append('\t')
                                            .append(entry.getValue().commitTimestampMs())
                                            .append('\n'));
            Files.writeString(tempFile, builder.toString(), StandardCharsets.UTF_8);
            try {
                Files.move(
                        tempFile,
                        snapshotFile,
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException ignored) {
                Files.move(tempFile, snapshotFile, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to persist offset snapshot " + snapshotFile, exception);
        }
    }

    private String encode(String value) {
        byte[] bytes = (value == null ? "" : value).getBytes(StandardCharsets.UTF_8);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    private String decode(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        return new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8);
    }
}
