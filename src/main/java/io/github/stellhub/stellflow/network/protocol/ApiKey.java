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
    SYNC_GROUP((short) 10),
    INIT_PRODUCER_ID((short) 11),
    BEGIN_TRANSACTION((short) 12),
    END_TRANSACTION((short) 13),
    CREATE_TOPIC((short) 50),
    DELETE_TOPIC((short) 51),
    ALTER_PARTITION((short) 52),
    DESCRIBE_CLUSTER((short) 53),
    HEALTH_CHECK((short) 54),
    DECOMMISSION_BROKER((short) 55);

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
