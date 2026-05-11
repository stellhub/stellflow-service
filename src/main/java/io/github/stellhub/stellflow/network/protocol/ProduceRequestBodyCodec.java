package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;

/**
 * Produce 请求体编解码器。
 */
public class ProduceRequestBodyCodec implements RequestBodyCodec<ProduceRequestBody> {

    @Override
    public ApiKey apiKey() {
        return ApiKey.PRODUCE;
    }

    @Override
    public short apiVersion() {
        return 0;
    }

    @Override
    public ProduceRequestBody decode(ByteBuf buffer) {
        String transactionalId = ProtocolSerde.readNullableString(buffer);
        short acks = buffer.readShort();
        int timeoutMs = buffer.readInt();
        int topicCount = buffer.readInt();
        List<ProduceTopicData> topicData = new ArrayList<>(Math.max(topicCount, 0));
        for (int topicIndex = 0; topicIndex < topicCount; topicIndex++) {
            String topic = ProtocolSerde.readNullableString(buffer);
            int partitionCount = buffer.readInt();
            List<ProducePartitionData> partitions = new ArrayList<>(Math.max(partitionCount, 0));
            for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
                int partition = buffer.readInt();
                byte[] records = ProtocolSerde.readBytes(buffer);
                partitions.add(new ProducePartitionData(partition, records));
            }
            topicData.add(new ProduceTopicData(topic, partitions));
        }
        return new ProduceRequestBody(transactionalId, acks, timeoutMs, topicData);
    }
}
