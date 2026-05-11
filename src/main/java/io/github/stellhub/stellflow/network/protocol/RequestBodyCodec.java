package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * 请求体编解码接口。
 */
public interface RequestBodyCodec<T extends RequestBody> {

    /**
     * 返回支持的 API。
     */
    ApiKey apiKey();

    /**
     * 返回支持的版本。
     */
    short apiVersion();

    /**
     * 解码请求体。
     */
    T decode(ByteBuf buffer);
}
