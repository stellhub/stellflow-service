package io.github.stellhub.stellflow.storage.log;

import java.util.List;

/**
 * 日志 FileRegion 读取视图。
 */
public record LogFileRegionReadResult(
        long highWatermark,
        long logStartOffset,
        long lastStableOffset,
        long nextFetchOffset,
        List<LogFileRegion> fileRegions,
        int readableBytes) {

    public LogFileRegionReadResult {
        fileRegions = fileRegions == null ? List.of() : List.copyOf(fileRegions);
        if (readableBytes < 0) {
            throw new IllegalArgumentException("readableBytes must not be negative");
        }
    }
}
