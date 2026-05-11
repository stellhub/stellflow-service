package io.github.stellhub.stellflow.network.protocol;

import java.util.List;

/**
 * ApiVersions 请求体。
 */
public record ApiVersionsRequestBody(
        String clientSoftwareName, String clientSoftwareVersion, List<String> supportedFeatures)
        implements RequestBody {}
