package io.github.stellhub.stellflow.network.protocol;

/**
 * 稳定 RecordBatch 的轻量元信息。
 */
public record RecordBatchInfo(
        boolean stableFormat,
        long producerId,
        short producerEpoch,
        int baseSequence,
        int recordCount,
        CompressionType compressionType,
        int payloadBytes,
        int uncompressedBytes,
        int fingerprint) {

    /**
     * 当前批次是否携带幂等 producer 元数据。
     */
    public boolean idempotent() {
        return stableFormat && producerId >= 0 && baseSequence >= 0 && recordCount > 0;
    }
}
