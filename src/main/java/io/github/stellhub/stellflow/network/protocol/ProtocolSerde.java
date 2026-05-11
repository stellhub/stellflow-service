package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * 协议基础序列化工具。
 */
public final class ProtocolSerde {

    private ProtocolSerde() {}

    /**
     * 读取 nullable string。
     */
    public static String readNullableString(ByteBuf buffer) {
        short length = buffer.readShort();
        if (length < 0) {
            return null;
        }
        byte[] value = new byte[length];
        buffer.readBytes(value);
        return new String(value, StandardCharsets.UTF_8);
    }

    /**
     * 写入 nullable string。
     */
    public static void writeNullableString(ByteBuf buffer, String value) {
        if (value == null) {
            buffer.writeShort(-1);
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        buffer.writeShort(bytes.length);
        buffer.writeBytes(bytes);
    }

    /**
     * 读取 string 数组。
     */
    public static List<String> readStringArray(ByteBuf buffer) {
        int length = buffer.readInt();
        List<String> values = new ArrayList<>(Math.max(length, 0));
        for (int index = 0; index < length; index++) {
            values.add(readNullableString(buffer));
        }
        return values;
    }

    /**
     * 写入 string 数组。
     */
    public static void writeStringArray(ByteBuf buffer, List<String> values) {
        buffer.writeInt(values.size());
        for (String value : values) {
            writeNullableString(buffer, value);
        }
    }

    /**
     * 读取 int32 数组。
     */
    public static List<Integer> readIntArray(ByteBuf buffer) {
        int length = buffer.readInt();
        List<Integer> values = new ArrayList<>(Math.max(length, 0));
        for (int index = 0; index < length; index++) {
            values.add(buffer.readInt());
        }
        return values;
    }

    /**
     * 写入 int32 数组。
     */
    public static void writeIntArray(ByteBuf buffer, List<Integer> values) {
        buffer.writeInt(values.size());
        for (Integer value : values) {
            buffer.writeInt(value);
        }
    }

    /**
     * 读取 bytes。
     */
    public static byte[] readBytes(ByteBuf buffer) {
        int length = buffer.readInt();
        if (length < 0) {
            return null;
        }
        byte[] bytes = new byte[length];
        buffer.readBytes(bytes);
        return bytes;
    }

    /**
     * 写入 bytes。
     */
    public static void writeBytes(ByteBuf buffer, byte[] value) {
        if (value == null) {
            buffer.writeInt(-1);
            return;
        }
        buffer.writeInt(value.length);
        buffer.writeBytes(value);
    }
}
