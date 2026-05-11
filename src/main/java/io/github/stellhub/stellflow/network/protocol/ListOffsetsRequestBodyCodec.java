package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;

/**
 * ListOffsets 请求体编解码器。
 */
public class ListOffsetsRequestBodyCodec implements RequestBodyCodec<ListOffsetsRequestBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.LIST_OFFSETS;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public ListOffsetsRequestBody decode(ByteBuf buffer) {
        int replicaId = buffer.readInt();
        byte isolationLevel = buffer.readByte();
        int topicCount = buffer.readInt();
        List<ListOffsetsTopicRequest> topics = new ArrayList<>(Math.max(topicCount, 0));
        for (int topicIndex = 0; topicIndex < topicCount; topicIndex++) {
            String topic = ProtocolSerde.readNullableString(buffer);
            int partitionCount = buffer.readInt();
            List<ListOffsetsPartitionRequest> partitions =
                    new ArrayList<>(Math.max(partitionCount, 0));
            for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
                partitions.add(
                        new ListOffsetsPartitionRequest(
                                buffer.readInt(),
                                buffer.readInt(),
                                buffer.readLong(),
                                buffer.readInt()));
            }
            topics.add(new ListOffsetsTopicRequest(topic, partitions));
        }
        return new ListOffsetsRequestBody(replicaId, isolationLevel, topics);
    }
}
