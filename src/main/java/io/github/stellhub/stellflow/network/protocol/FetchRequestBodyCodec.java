package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;

/**
 * Fetch 请求体编解码器。
 */
public class FetchRequestBodyCodec implements RequestBodyCodec<FetchRequestBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.FETCH;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public FetchRequestBody decode(ByteBuf buffer) {
        int replicaId = buffer.readInt();
        int maxWaitMs = buffer.readInt();
        int minBytes = buffer.readInt();
        int maxBytes = buffer.readInt();
        byte isolationLevel = buffer.readByte();
        int sessionId = buffer.readInt();
        int topicCount = buffer.readInt();
        List<FetchTopicRequest> topics = new ArrayList<>(Math.max(topicCount, 0));
        for (int topicIndex = 0; topicIndex < topicCount; topicIndex++) {
            String topic = ProtocolSerde.readNullableString(buffer);
            int partitionCount = buffer.readInt();
            List<FetchPartitionRequest> partitions = new ArrayList<>(Math.max(partitionCount, 0));
            for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
                partitions.add(
                        new FetchPartitionRequest(
                                buffer.readInt(),
                                buffer.readInt(),
                                buffer.readLong(),
                                buffer.readLong(),
                                buffer.readInt()));
            }
            topics.add(new FetchTopicRequest(topic, partitions));
        }
        return new FetchRequestBody(
                replicaId, maxWaitMs, minBytes, maxBytes, isolationLevel, sessionId, topics);
    }
}
