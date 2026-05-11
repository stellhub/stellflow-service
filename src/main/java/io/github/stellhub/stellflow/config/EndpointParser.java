package io.github.stellhub.stellflow.config;

import java.net.URI;

/**
 * 端点解析工具。
 */
public final class EndpointParser {

    private EndpointParser() {}

    /**
     * 解析带 scheme 的端点配置。
     */
    public static ParsedEndpoint parse(String endpoint, String expectedScheme) {
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalArgumentException("endpoint must not be blank");
        }
        URI uri = URI.create(endpoint.trim());
        if (uri.getScheme() == null || !expectedScheme.equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException(
                    "Expected scheme "
                            + expectedScheme
                            + " for endpoint "
                            + endpoint
                            + ", but was "
                            + uri.getScheme());
        }
        if (uri.getHost() == null || uri.getHost().isBlank()) {
            throw new IllegalArgumentException("Endpoint host must not be blank: " + endpoint);
        }
        if (uri.getPort() <= 0) {
            throw new IllegalArgumentException("Endpoint port must be positive: " + endpoint);
        }
        return new ParsedEndpoint(uri.getScheme(), uri.getHost(), uri.getPort());
    }

    /**
     * 解析兼容旧格式的 host:port 或新格式的带 scheme endpoint。
     */
    public static ParsedEndpoint parseLenient(
            String endpointOrHostPort, String expectedScheme, String defaultScheme) {
        if (endpointOrHostPort == null || endpointOrHostPort.isBlank()) {
            throw new IllegalArgumentException("endpointOrHostPort must not be blank");
        }
        String value = endpointOrHostPort.trim();
        if (value.contains("://")) {
            return parse(value, expectedScheme);
        }
        int separator = value.lastIndexOf(':');
        if (separator <= 0 || separator == value.length() - 1) {
            throw new IllegalArgumentException("Expected host:port, but was " + value);
        }
        String host = value.substring(0, separator);
        int port = Integer.parseInt(value.substring(separator + 1));
        if (port <= 0) {
            throw new IllegalArgumentException("Endpoint port must be positive: " + value);
        }
        return new ParsedEndpoint(defaultScheme, host, port);
    }

    /**
     * 已解析端点。
     */
    public record ParsedEndpoint(String scheme, String host, int port) {}
}
