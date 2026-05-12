package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * OffsetFetch 响应体编解码器。
 */
public class OffsetFetchResponseBodyCodec implements ResponseBodyCodec<OffsetFetchResponseBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.OFFSET_FETCH;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public Class<OffsetFetchResponseBody> bodyType() {
        return OffsetFetchResponseBody.class;
    }

    @Override
    public void encode(OffsetFetchResponseBody body, ByteBuf buffer) {
        buffer.writeInt(body.topics().size());
        for (OffsetFetchTopicResponse topic : body.topics()) {
            ProtocolSerde.writeNullableString(buffer, topic.topic());
            buffer.writeInt(topic.partitions().size());
            for (OffsetFetchPartitionResponse partition : topic.partitions()) {
                buffer.writeInt(partition.partition());
                buffer.writeLong(partition.offset());
                ProtocolSerde.writeNullableString(buffer, partition.metadata());
                buffer.writeShort(partition.errorCode().code());
            }
        }
    }
}
