package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * OffsetCommit 响应体编解码器。
 */
public class OffsetCommitResponseBodyCodec implements ResponseBodyCodec<OffsetCommitResponseBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.OFFSET_COMMIT;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public Class<OffsetCommitResponseBody> bodyType() {
        return OffsetCommitResponseBody.class;
    }

    @Override
    public void encode(OffsetCommitResponseBody body, ByteBuf buffer) {
        buffer.writeInt(body.topics().size());
        for (OffsetCommitTopicResponse topic : body.topics()) {
            ProtocolSerde.writeNullableString(buffer, topic.topic());
            buffer.writeInt(topic.partitions().size());
            for (OffsetCommitPartitionResponse partition : topic.partitions()) {
                buffer.writeInt(partition.partition());
                buffer.writeShort(partition.errorCode().code());
            }
        }
    }
}
