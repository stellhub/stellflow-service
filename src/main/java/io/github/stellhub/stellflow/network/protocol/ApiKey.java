package io.github.stellhub.stellflow.network.protocol;

/**
 * 数据面 API 标识。
 */
public enum ApiKey {
    UNKNOWN(Short.MIN_VALUE),
    API_VERSIONS((short) 0),
    METADATA((short) 1),
    PRODUCE((short) 2),
    FETCH((short) 3),
    LIST_OFFSETS((short) 4),
    OFFSET_COMMIT((short) 5),
    OFFSET_FETCH((short) 6),
    FIND_COORDINATOR((short) 7),
    HEARTBEAT((short) 8),
    JOIN_GROUP((short) 9),
    SYNC_GROUP((short) 10);

    private final short code;

    ApiKey(short code) {
        this.code = code;
    }

    /**
     * 返回协议编码值。
     */
    public short code() {
        return code;
    }

    /**
     * 根据编码解析 API 标识。
     */
    public static ApiKey fromCode(short code) {
        for (ApiKey value : values()) {
            if (value.code == code) {
                return value;
            }
        }
        return UNKNOWN;
    }
}
