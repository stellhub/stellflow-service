package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * ApiVersions 响应体编解码器。
 */
public class ApiVersionsResponseBodyCodec implements ResponseBodyCodec<ApiVersionsResponseBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.API_VERSIONS;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public Class<ApiVersionsResponseBody> bodyType() {
        return ApiVersionsResponseBody.class;
    }

    @Override
    public void encode(ApiVersionsResponseBody body, ByteBuf buffer) {
        buffer.writeInt(body.apiVersions().size());
        for (ApiVersionRange apiVersion : body.apiVersions()) {
            buffer.writeShort(apiVersion.apiKey().code());
            buffer.writeShort(apiVersion.minVersion());
            buffer.writeShort(apiVersion.maxVersion());
        }
        ProtocolSerde.writeNullableString(buffer, body.brokerSoftwareName());
        ProtocolSerde.writeNullableString(buffer, body.brokerSoftwareVersion());
        ProtocolSerde.writeStringArray(buffer, body.supportedFeatures());
    }
}
