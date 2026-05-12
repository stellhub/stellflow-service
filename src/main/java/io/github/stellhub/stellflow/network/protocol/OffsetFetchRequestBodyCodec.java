package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;

/**
 * OffsetFetch 请求体编解码器。
 */
public class OffsetFetchRequestBodyCodec implements RequestBodyCodec<OffsetFetchRequestBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.OFFSET_FETCH;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public OffsetFetchRequestBody decode(ByteBuf buffer) {
        String groupId = ProtocolSerde.readNullableString(buffer);
        int topicCount = buffer.readInt();
        List<OffsetFetchTopic> topics = new ArrayList<>(Math.max(topicCount, 0));
        for (int topicIndex = 0; topicIndex < topicCount; topicIndex++) {
            String topic = ProtocolSerde.readNullableString(buffer);
            int partitionCount = buffer.readInt();
            List<OffsetFetchPartition> partitions = new ArrayList<>(Math.max(partitionCount, 0));
            for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
                partitions.add(new OffsetFetchPartition(buffer.readInt()));
            }
            topics.add(new OffsetFetchTopic(topic, partitions));
        }
        return new OffsetFetchRequestBody(groupId, topics);
    }
}
