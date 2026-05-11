package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * Produce 响应体编码器。
 */
public class ProduceResponseBodyCodec implements ResponseBodyCodec<ProduceResponseBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.PRODUCE;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public Class<ProduceResponseBody> bodyType() {
        return ProduceResponseBody.class;
    }

    @Override
    public void encode(ProduceResponseBody body, ByteBuf buffer) {
        buffer.writeInt(body.responses().size());
        for (ProduceTopicResponse topicResponse : body.responses()) {
            ProtocolSerde.writeNullableString(buffer, topicResponse.topic());
            buffer.writeInt(topicResponse.partitions().size());
            for (ProducePartitionResponse partitionResponse : topicResponse.partitions()) {
                buffer.writeInt(partitionResponse.partition());
                buffer.writeShort(partitionResponse.errorCode().code());
                buffer.writeLong(partitionResponse.baseOffset());
                buffer.writeInt(partitionResponse.currentLeaderEpoch());
                buffer.writeLong(partitionResponse.logAppendTimeMs());
                buffer.writeLong(partitionResponse.logStartOffset());
            }
        }
    }
}
