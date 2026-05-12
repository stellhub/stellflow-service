package io.github.stellhub.stellflow.security;

import io.github.stellhub.stellflow.server.api.RequestContext;

/**
 * 默认放行认证器。
 */
public class AllowAllAuthenticator implements Authenticator {

    @Override
    public AuthenticationResult authenticate(RequestContext requestContext) {
        String principal =
                requestContext.getAuthContextId() == null || requestContext.getAuthContextId().isBlank()
                        ? "anonymous"
                        : requestContext.getAuthContextId();
        return AuthenticationResult.allow(principal);
    }
}
