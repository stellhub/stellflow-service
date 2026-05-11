package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * ApiVersions 请求体编解码器。
 */
public class ApiVersionsRequestBodyCodec implements RequestBodyCodec<ApiVersionsRequestBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.API_VERSIONS;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public ApiVersionsRequestBody decode(ByteBuf buffer) {
        return new ApiVersionsRequestBody(
                ProtocolSerde.readNullableString(buffer),
                ProtocolSerde.readNullableString(buffer),
                ProtocolSerde.readStringArray(buffer));
    }
}
