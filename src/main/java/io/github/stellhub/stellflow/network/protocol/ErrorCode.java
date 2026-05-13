package io.github.stellhub.stellflow.network.protocol;

/**
 * 协议顶层错误码。
 */
public enum ErrorCode {
    NONE((short) 0),
    UNKNOWN_SERVER_ERROR((short) 1),
    UNSUPPORTED_VERSION((short) 2),
    INVALID_REQUEST((short) 3),
    AUTHENTICATION_FAILED((short) 4),
    AUTHORIZATION_FAILED((short) 5),
    THROTTLED((short) 6),
    BROKER_NOT_AVAILABLE((short) 7),
    LEADER_NOT_AVAILABLE((short) 8),
    NOT_LEADER_OR_FOLLOWER((short) 9),
    UNKNOWN_TOPIC_OR_PARTITION((short) 10),
    OFFSET_OUT_OF_RANGE((short) 11),
    MESSAGE_TOO_LARGE((short) 12),
    RECORD_LIST_TOO_LARGE((short) 13),
    INVALID_RECORD((short) 14),
    CORRUPT_MESSAGE((short) 15),
    COORDINATOR_NOT_AVAILABLE((short) 16),
    NOT_COORDINATOR((short) 17),
    CONCURRENT_TRANSACTIONS((short) 18),
    FENCED_INSTANCE_ID((short) 19),
    FEATURE_NOT_ENABLED((short) 20),
    OUT_OF_ORDER_SEQUENCE((short) 21),
    DUPLICATE_SEQUENCE((short) 22);

    private final short code;

    ErrorCode(short code) {
        this.code = code;
    }

    /**
     * 返回错误码值。
     */
    public short code() {
        return code;
    }

    /**
     * 根据编码解析错误码。
     */
    public static ErrorCode fromCode(short code) {
        for (ErrorCode value : values()) {
            if (value.code == code) {
                return value;
            }
        }
        return UNKNOWN_SERVER_ERROR;
    }
}
