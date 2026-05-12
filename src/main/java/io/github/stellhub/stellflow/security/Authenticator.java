package io.github.stellhub.stellflow.security;

import io.github.stellhub.stellflow.server.api.RequestContext;

/**
 * 请求认证器。
 */
public interface Authenticator {

    /**
     * 认证请求。
     */
    AuthenticationResult authenticate(RequestContext requestContext);
}
