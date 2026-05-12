package io.github.stellhub.stellflow.controller.quorum;

import io.github.stellhub.stellflow.controller.control.ControllerMetadataStateMachine;
import io.github.stellhub.stellflow.controller.control.ControllerPartitionMetadata;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Controller 元数据记录编解码器。
 */
public final class ControllerMetadataRecordCodec {

    private ControllerMetadataRecordCodec() {}

    /**
     * 编码记录。
     */
    public static byte[] encode(ControllerMetadataRecord record) {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutput = new DataOutputStream(outputStream);
            dataOutput.writeInt(record.type().ordinal());
            writeNullableInt(dataOutput, record.brokerId());
            writeNullableString(dataOutput, record.advertisedEndpoint());
            writeNullableString(dataOutput, record.advertisedHost());
            writeNullableInt(dataOutput, record.advertisedPort());
            writeNullableLong(dataOutput, record.registeredAtMs());
            writeNullableString(dataOutput, record.topic());
            writeNullableInt(dataOutput, record.partitionCount());
            writeNullableLong(dataOutput, record.topicCreatedAtMs());
            writeNullableInt(dataOutput, record.partition());
            writeNullableInt(dataOutput, record.leaderId());
            writeNullableInt(dataOutput, record.leaderEpoch());
            writeIntegerList(dataOutput, record.replicaNodes());
            writeIntegerList(dataOutput, record.isrNodes());
            writeNullableInt(dataOutput, record.truncateToLeaderEpoch());
            writeNullableLong(dataOutput, record.truncateToOffset());
            dataOutput.flush();
            return outputStream.toByteArray();
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to encode controller metadata record", exception);
        }
    }

    /**
     * 解码记录。
     */
    public static ControllerMetadataRecord decode(byte[] bytes) {
        try {
            DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(bytes));
            ControllerMetadataRecordType type =
                    ControllerMetadataRecordType.values()[inputStream.readInt()];
            return ControllerMetadataRecord.builder()
                    .type(type)
                    .brokerId(readNullableInt(inputStream))
                    .advertisedEndpoint(readNullableString(inputStream))
                    .advertisedHost(readNullableString(inputStream))
                    .advertisedPort(readNullableInt(inputStream))
                    .registeredAtMs(readNullableLong(inputStream))
                    .topic(readNullableString(inputStream))
                    .partitionCount(readNullableInt(inputStream))
                    .topicCreatedAtMs(readNullableLong(inputStream))
                    .partition(readNullableInt(inputStream))
                    .leaderId(readNullableInt(inputStream))
                    .leaderEpoch(readNullableInt(inputStream))
                    .replicaNodes(readIntegerList(inputStream))
                    .isrNodes(readIntegerList(inputStream))
                    .truncateToLeaderEpoch(readNullableInt(inputStream))
                    .truncateToOffset(readNullableLong(inputStream))
                    .build();
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to decode controller metadata record", exception);
        }
    }

    /**
     * 将日志记录还原成分区元数据。
     */
    public static ControllerPartitionMetadata toPartitionMetadata(ControllerMetadataRecord record) {
        return ControllerMetadataStateMachine.partition(
                record.topic(),
                record.partition(),
                record.leaderId(),
                record.leaderEpoch(),
                record.replicaNodes(),
                record.isrNodes(),
                record.truncateToLeaderEpoch(),
                record.truncateToOffset());
    }

    private static void writeNullableString(DataOutputStream output, String value) throws IOException {
        if (value == null) {
            output.writeBoolean(false);
            return;
        }
        output.writeBoolean(true);
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readNullableString(DataInputStream input) throws IOException {
        if (!input.readBoolean()) {
            return null;
        }
        byte[] bytes = input.readNBytes(input.readInt());
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static void writeNullableInt(DataOutputStream output, Integer value) throws IOException {
        if (value == null) {
            output.writeBoolean(false);
            return;
        }
        output.writeBoolean(true);
        output.writeInt(value);
    }

    private static Integer readNullableInt(DataInputStream input) throws IOException {
        return input.readBoolean() ? input.readInt() : null;
    }

    private static void writeNullableLong(DataOutputStream output, Long value) throws IOException {
        if (value == null) {
            output.writeBoolean(false);
            return;
        }
        output.writeBoolean(true);
        output.writeLong(value);
    }

    private static Long readNullableLong(DataInputStream input) throws IOException {
        return input.readBoolean() ? input.readLong() : null;
    }

    private static void writeIntegerList(DataOutputStream output, List<Integer> values) throws IOException {
        if (values == null) {
            output.writeInt(-1);
            return;
        }
        output.writeInt(values.size());
        for (Integer value : values) {
            output.writeInt(value);
        }
    }

    private static List<Integer> readIntegerList(DataInputStream input) throws IOException {
        int size = input.readInt();
        if (size < 0) {
            return null;
        }
        List<Integer> values = new ArrayList<>(size);
        for (int index = 0; index < size; index++) {
            values.add(input.readInt());
        }
        return List.copyOf(values);
    }
}
