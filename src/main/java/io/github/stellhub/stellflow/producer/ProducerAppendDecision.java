package io.github.stellhub.stellflow.producer;

import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.network.protocol.RecordBatchInfo;

/**
 * Producer 追加前判定结果。
 */
public record ProducerAppendDecision(
        ErrorCode errorCode,
        boolean appendRequired,
        long duplicateBaseOffset,
        RecordBatchInfo batchInfo) {

    /**
     * 返回允许追加的判定。
     */
    public static ProducerAppendDecision append(RecordBatchInfo batchInfo) {
        return new ProducerAppendDecision(ErrorCode.NONE, true, -1, batchInfo);
    }

    /**
     * 返回重复批次判定。
     */
    public static ProducerAppendDecision duplicate(long baseOffset, RecordBatchInfo batchInfo) {
        return new ProducerAppendDecision(ErrorCode.NONE, false, baseOffset, batchInfo);
    }

    /**
     * 返回拒绝追加的判定。
     */
    public static ProducerAppendDecision reject(ErrorCode errorCode, RecordBatchInfo batchInfo) {
        return new ProducerAppendDecision(errorCode, false, -1, batchInfo);
    }
}
