package io.github.stellhub.stellflow.storage.log;

import java.nio.file.Path;
import java.util.Objects;

/**
 * 日志文件中的可传输 payload 区间。
 */
public record LogFileRegion(Path file, long position, long count) {

    public LogFileRegion {
        Objects.requireNonNull(file, "file");
        if (position < 0) {
            throw new IllegalArgumentException("position must not be negative");
        }
        if (count < 0) {
            throw new IllegalArgumentException("count must not be negative");
        }
    }
}
