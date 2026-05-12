package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * Topic 管理请求体编解码器。
 */
public class TopicAdminRequestBodyCodec implements RequestBodyCodec<TopicAdminRequestBody> {

    private final ApiKey apiKey;

    public TopicAdminRequestBodyCodec(ApiKey apiKey) {
        this.apiKey = apiKey;
    }

    @Override
    public ApiKey apiKey() {
        return apiKey;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public TopicAdminRequestBody decode(ByteBuf buffer) {
        return new TopicAdminRequestBody(
                ProtocolSerde.readNullableString(buffer),
                buffer.readInt(),
                buffer.readInt(),
                buffer.readInt(),
                buffer.readInt(),
                ProtocolSerde.readIntArray(buffer),
                ProtocolSerde.readIntArray(buffer));
    }
}
