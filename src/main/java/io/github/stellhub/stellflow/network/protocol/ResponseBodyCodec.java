package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * 响应体编码接口。
 */
public interface ResponseBodyCodec<T extends ResponseBody> {

    /**
     * 返回支持的 API。
     */
    ApiKey apiKey();

    /**
     * 返回支持的版本。
     */
    short apiVersion();

    /**
     * 返回响应体类型。
     */
    Class<T> bodyType();

    /**
     * 编码响应体。
     */
    void encode(T body, ByteBuf buffer);
}
