package io.github.stellhub.stellflow.server.api;

import java.util.List;

/**
 * Fetch records 字段的零拷贝文件区间。
 */
public record FetchRecordsFileRegion(
        String topic, int partition, int readableBytes, List<ZeroCopyFileRegion> fileRegions) {

    public FetchRecordsFileRegion {
        fileRegions = fileRegions == null ? List.of() : List.copyOf(fileRegions);
        if (readableBytes < 0) {
            throw new IllegalArgumentException("readableBytes must not be negative");
        }
    }

    /**
     * 判断是否匹配指定分区。
     */
    public boolean matches(String topic, int partition) {
        return this.partition == partition && java.util.Objects.equals(this.topic, topic);
    }
}
