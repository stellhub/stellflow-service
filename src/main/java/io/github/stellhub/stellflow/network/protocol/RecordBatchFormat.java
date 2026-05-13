package io.github.stellhub.stellflow.network.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.zip.CRC32;
import java.util.zip.GZIPInputStream;

/**
 * RecordBatch 稳定格式编解码辅助。
 */
public final class RecordBatchFormat {

    public static final int MAGIC = 0x53524642;
    public static final short VERSION = 1;
    private static final int HEADER_BYTES =
            Integer.BYTES
                    + Short.BYTES
                    + Byte.BYTES
                    + Long.BYTES
                    + Short.BYTES
                    + Integer.BYTES
                    + Integer.BYTES
                    + Integer.BYTES
                    + Integer.BYTES
                    + Integer.BYTES;

    private RecordBatchFormat() {}

    /**
     * 尝试读取稳定 RecordBatch 元信息。
     */
    public static Optional<RecordBatchInfo> tryReadInfo(byte[] records) {
        if (records == null || records.length < Integer.BYTES) {
            return Optional.empty();
        }
        ByteBuffer magicBuffer = ByteBuffer.wrap(records, 0, Integer.BYTES);
        if (magicBuffer.getInt() != MAGIC) {
            return Optional.empty();
        }
        if (records.length < HEADER_BYTES) {
            throw new ProtocolEncodingException("RecordBatch header is incomplete");
        }
        ByteBuffer buffer = ByteBuffer.wrap(records);
        int magic = buffer.getInt();
        if (magic != MAGIC) {
            return Optional.empty();
        }
        short version = buffer.getShort();
        if (version != VERSION) {
            throw new ProtocolEncodingException("Unsupported RecordBatch version: " + version);
        }
        CompressionType compressionType = CompressionType.fromCode(buffer.get());
        long producerId = buffer.getLong();
        short producerEpoch = buffer.getShort();
        int baseSequence = buffer.getInt();
        int recordCount = buffer.getInt();
        int uncompressedLength = buffer.getInt();
        int payloadLength = buffer.getInt();
        int expectedCrc = buffer.getInt();
        if (recordCount <= 0 || uncompressedLength < 0 || payloadLength < 0) {
            throw new ProtocolEncodingException("Invalid RecordBatch length metadata");
        }
        if (HEADER_BYTES + payloadLength != records.length) {
            throw new ProtocolEncodingException("RecordBatch payload length mismatch");
        }
        byte[] payload = new byte[payloadLength];
        buffer.get(payload);
        int actualCrc = crc32(payload);
        if (actualCrc != expectedCrc) {
            throw new ProtocolEncodingException("RecordBatch CRC validation failed");
        }
        int actualUncompressedLength = uncompressedLength(payload, compressionType);
        if (actualUncompressedLength != uncompressedLength) {
            throw new ProtocolEncodingException("RecordBatch uncompressed length mismatch");
        }
        return Optional.of(
                new RecordBatchInfo(
                        true,
                        producerId,
                        producerEpoch,
                        baseSequence,
                        recordCount,
                        compressionType,
                        payloadLength,
                        uncompressedLength,
                        actualCrc));
    }

    /**
     * 计算 records 指纹，稳定格式优先使用载荷 CRC。
     */
    public static int fingerprint(byte[] records) {
        Optional<RecordBatchInfo> info = tryReadInfo(records);
        return info.map(RecordBatchInfo::fingerprint).orElseGet(() -> crc32(records));
    }

    private static int uncompressedLength(byte[] payload, CompressionType compressionType) {
        if (compressionType == CompressionType.NONE) {
            return payload.length;
        }
        if (compressionType == CompressionType.GZIP) {
            try (GZIPInputStream inputStream = new GZIPInputStream(new ByteArrayInputStream(payload));
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                inputStream.transferTo(outputStream);
                return outputStream.size();
            } catch (IOException exception) {
                throw new ProtocolEncodingException("Failed to decompress RecordBatch payload", exception);
            }
        }
        throw new ProtocolEncodingException("Unsupported RecordBatch compression: " + compressionType);
    }

    private static int crc32(byte[] bytes) {
        CRC32 crc32 = new CRC32();
        crc32.update(bytes == null ? new byte[0] : bytes);
        return (int) crc32.getValue();
    }
}
