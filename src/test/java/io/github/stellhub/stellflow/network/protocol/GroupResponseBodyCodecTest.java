package io.github.stellhub.stellflow.network.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * GroupResponseBodyCodec 测试。
 */
class GroupResponseBodyCodecTest {

    /**
     * 验证 SyncGroup 响应会编码 assignment。
     */
    @Test
    void shouldEncodeSyncGroupAssignments() {
        ByteBuf buffer = Unpooled.buffer();
        GroupResponseBodyCodec codec = new GroupResponseBodyCodec(ApiKey.SYNC_GROUP);

        codec.encode(
                new SyncGroupResponseBody(
                        ErrorCode.NONE,
                        List.of(
                                new ConsumerPartitionAssignment("orders", 0),
                                new ConsumerPartitionAssignment("payments", 3))),
                buffer);

        assertEquals(ErrorCode.NONE.code(), buffer.readShort());
        assertEquals(2, buffer.readInt());
        assertEquals("orders", ProtocolSerde.readNullableString(buffer));
        assertEquals(0, buffer.readInt());
        assertEquals("payments", ProtocolSerde.readNullableString(buffer));
        assertEquals(3, buffer.readInt());
    }
}
