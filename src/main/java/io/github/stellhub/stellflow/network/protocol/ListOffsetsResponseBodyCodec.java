package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * ListOffsets 响应体编码器。
 */
public class ListOffsetsResponseBodyCodec
        implements ResponseBodyCodec<ListOffsetsResponseBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.LIST_OFFSETS;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public Class<ListOffsetsResponseBody> bodyType() {
        return ListOffsetsResponseBody.class;
    }

    @Override
    public void encode(ListOffsetsResponseBody body, ByteBuf buffer) {
        buffer.writeInt(body.topics().size());
        for (ListOffsetsTopicResponse topic : body.topics()) {
            ProtocolSerde.writeNullableString(buffer, topic.topic());
            buffer.writeInt(topic.partitions().size());
            for (ListOffsetsPartitionResponse partition : topic.partitions()) {
                buffer.writeInt(partition.partition());
                buffer.writeShort(partition.errorCode().code());
                buffer.writeInt(partition.leaderEpoch());
                buffer.writeLong(partition.timestamp());
                buffer.writeLong(partition.offset());
                buffer.writeInt(partition.offsets().size());
                for (Long offset : partition.offsets()) {
                    buffer.writeLong(offset);
                }
            }
        }
    }
}
