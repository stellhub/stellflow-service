package io.github.stellhub.stellflow.server.api;

import java.nio.file.Path;

/**
 * 零拷贝文件区域描述。
 */
public record ZeroCopyFileRegion(Path file, long position, long count) {}
