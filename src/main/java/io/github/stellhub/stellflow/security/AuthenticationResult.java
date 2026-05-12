package io.github.stellhub.stellflow.security;

/**
 * 认证结果。
 */
public record AuthenticationResult(boolean allowed, String principal, String message) {

    public static AuthenticationResult allow(String principal) {
        return new AuthenticationResult(true, principal, "allowed");
    }

    public static AuthenticationResult deny(String message) {
        return new AuthenticationResult(false, null, message);
    }
}
