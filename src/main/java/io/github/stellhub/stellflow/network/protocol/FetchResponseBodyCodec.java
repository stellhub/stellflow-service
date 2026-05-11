package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * Fetch 响应体编码器。
 */
public class FetchResponseBodyCodec implements ResponseBodyCodec<FetchResponseBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.FETCH;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public Class<FetchResponseBody> bodyType() {
        return FetchResponseBody.class;
    }

    @Override
    public void encode(FetchResponseBody body, ByteBuf buffer) {
        buffer.writeInt(body.sessionId());
        buffer.writeInt(body.responses().size());
        for (FetchTopicResponse topicResponse : body.responses()) {
            ProtocolSerde.writeNullableString(buffer, topicResponse.topic());
            buffer.writeInt(topicResponse.partitions().size());
            for (FetchPartitionResponse partitionResponse : topicResponse.partitions()) {
                buffer.writeInt(partitionResponse.partition());
                buffer.writeShort(partitionResponse.errorCode().code());
                buffer.writeLong(partitionResponse.highWatermark());
                buffer.writeLong(partitionResponse.logStartOffset());
                buffer.writeLong(partitionResponse.lastStableOffset());
                buffer.writeInt(partitionResponse.abortedTransactions().size());
                for (AbortedTransaction abortedTransaction : partitionResponse.abortedTransactions()) {
                    buffer.writeLong(abortedTransaction.producerId());
                    buffer.writeLong(abortedTransaction.firstOffset());
                }
                ProtocolSerde.writeBytes(buffer, partitionResponse.records());
            }
        }
    }
}
