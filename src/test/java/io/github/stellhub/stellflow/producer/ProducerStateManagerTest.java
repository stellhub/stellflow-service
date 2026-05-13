package io.github.stellhub.stellflow.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.stellhub.stellflow.network.protocol.CompressionType;
import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.network.protocol.RecordBatchFormat;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;
import org.junit.jupiter.api.Test;

/**
 * ProducerStateManager 测试。
 */
class ProducerStateManagerTest {

    /**
     * 验证幂等批次成功后重复提交会被去重。
     */
    @Test
    void shouldDeduplicateRepeatedStableBatch() {
        ProducerStateManager manager = new ProducerStateManager();
        byte[] batch = stableBatch(7L, (short) 0, 0, 2, "ab");

        ProducerAppendDecision first = manager.validateAppend("tx-a", "orders", 0, batch);
        assertEquals(ErrorCode.NONE, first.errorCode());
        assertTrue(first.appendRequired());
        manager.recordAppendSuccess(first.batchInfo(), "orders", 0, 42L);

        ProducerAppendDecision duplicate = manager.validateAppend("tx-a", "orders", 0, batch);
        assertEquals(ErrorCode.NONE, duplicate.errorCode());
        assertFalse(duplicate.appendRequired());
        assertEquals(42L, duplicate.duplicateBaseOffset());
    }

    /**
     * 验证乱序序列号会被拒绝。
     */
    @Test
    void shouldRejectOutOfOrderSequence() {
        ProducerStateManager manager = new ProducerStateManager();
        byte[] batch = stableBatch(7L, (short) 0, 3, 1, "late");

        ProducerAppendDecision decision = manager.validateAppend("tx-a", "orders", 0, batch);

        assertEquals(ErrorCode.OUT_OF_ORDER_SEQUENCE, decision.errorCode());
        assertFalse(decision.appendRequired());
    }

    private static byte[] stableBatch(
            long producerId, short producerEpoch, int baseSequence, int recordCount, String payload) {
        byte[] payloadBytes = payload.getBytes(StandardCharsets.UTF_8);
        CRC32 crc32 = new CRC32();
        crc32.update(payloadBytes);
        ByteBuffer buffer =
                ByteBuffer.allocate(
                        Integer.BYTES
                                + Short.BYTES
                                + Byte.BYTES
                                + Long.BYTES
                                + Short.BYTES
                                + Integer.BYTES
                                + Integer.BYTES
                                + Integer.BYTES
                                + Integer.BYTES
                                + Integer.BYTES
                                + payloadBytes.length);
        buffer.putInt(RecordBatchFormat.MAGIC);
        buffer.putShort(RecordBatchFormat.VERSION);
        buffer.put(CompressionType.NONE.code());
        buffer.putLong(producerId);
        buffer.putShort(producerEpoch);
        buffer.putInt(baseSequence);
        buffer.putInt(recordCount);
        buffer.putInt(payloadBytes.length);
        buffer.putInt(payloadBytes.length);
        buffer.putInt((int) crc32.getValue());
        buffer.put(payloadBytes);
        return buffer.array();
    }
}
