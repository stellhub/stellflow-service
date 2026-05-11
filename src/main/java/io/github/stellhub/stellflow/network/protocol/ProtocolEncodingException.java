package io.github.stellhub.stellflow.network.protocol;

/**
 * 协议编解码异常。
 */
public class ProtocolEncodingException extends RuntimeException {

    public ProtocolEncodingException(String message) {
        super(message);
    }

    public ProtocolEncodingException(String message, Throwable cause) {
        super(message, cause);
    }
}
