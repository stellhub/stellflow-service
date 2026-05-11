package io.github.stellhub.stellflow.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * 协议头编解码器。
 */
public final class HeaderCodec {

    private HeaderCodec() {}

    /**
     * 解码请求头。
     */
    public static RequestHeader decodeRequestHeader(ByteBuf buffer) {
        return new RequestHeader(
                buffer.readShort(),
                buffer.readShort(),
                buffer.readShort(),
                buffer.readInt(),
                ProtocolSerde.readNullableString(buffer),
                ProtocolSerde.readNullableString(buffer),
                ProtocolSerde.readNullableString(buffer),
                buffer.readByte(),
                ProtocolSerde.readNullableString(buffer),
                ProtocolSerde.readNullableString(buffer),
                ProtocolSerde.readNullableString(buffer),
                buffer.readByte(),
                ProtocolSerde.readNullableString(buffer),
                buffer.readShort());
    }

    /**
     * 编码响应头。
     */
    public static void encodeResponseHeader(ByteBuf buffer, ResponseHeader header) {
        buffer.writeInt(header.correlationId());
        buffer.writeShort(header.headerVersion());
        buffer.writeShort(header.errorCode().code());
        buffer.writeInt(header.throttleTimeMs());
    }
}
