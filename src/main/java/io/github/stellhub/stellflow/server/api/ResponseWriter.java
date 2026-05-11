package io.github.stellhub.stellflow.server.api;

/**
 * 响应写出接口。
 */
public interface ResponseWriter {

    /**
     * 写出响应。
     */
    void write(ResponseContext responseContext);
}
