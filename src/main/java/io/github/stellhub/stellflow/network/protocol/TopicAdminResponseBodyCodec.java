package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * Topic 管理响应体编解码器。
 */
public class TopicAdminResponseBodyCodec implements ResponseBodyCodec<TopicAdminResponseBody> {

    private final ApiKey apiKey;

    public TopicAdminResponseBodyCodec(ApiKey apiKey) {
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
    public Class<TopicAdminResponseBody> bodyType() {
        return TopicAdminResponseBody.class;
    }

    @Override
    public void encode(TopicAdminResponseBody body, ByteBuf buffer) {
        ProtocolSerde.writeNullableString(buffer, body.topic());
        buffer.writeInt(body.partitions().size());
        for (TopicAdminPartitionResponse partition : body.partitions()) {
            buffer.writeInt(partition.partition());
            buffer.writeShort(partition.errorCode().code());
            buffer.writeInt(partition.leaderEpoch());
        }
    }
}
