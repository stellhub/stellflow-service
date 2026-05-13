package io.github.stellhub.stellflow.network.protocol;

/**
 * RecordBatch 压缩类型。
 */
public enum CompressionType {
    NONE((byte) 0),
    GZIP((byte) 1);

    private final byte code;

    CompressionType(byte code) {
        this.code = code;
    }

    /**
     * 返回协议编码。
     */
    public byte code() {
        return code;
    }

    /**
     * 根据协议编码解析压缩类型。
     */
    public static CompressionType fromCode(byte code) {
        for (CompressionType value : values()) {
            if (value.code == code) {
                return value;
            }
        }
        throw new IllegalArgumentException("Unsupported compression type: " + code);
    }
}
