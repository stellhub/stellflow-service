package io.github.stellhub.stellflow.network.protocol;

/**
 * API 版本范围。
 */
public record ApiVersionRange(ApiKey apiKey, short minVersion, short maxVersion) {}
