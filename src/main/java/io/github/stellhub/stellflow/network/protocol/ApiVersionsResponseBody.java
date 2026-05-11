package io.github.stellhub.stellflow.network.protocol;

import java.util.List;

/**
 * ApiVersions 响应体。
 */
public record ApiVersionsResponseBody(
        List<ApiVersionRange> apiVersions,
        String brokerSoftwareName,
        String brokerSoftwareVersion,
        List<String> supportedFeatures)
        implements ResponseBody {}
