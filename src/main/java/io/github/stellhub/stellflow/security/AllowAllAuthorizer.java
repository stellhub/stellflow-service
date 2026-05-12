package io.github.stellhub.stellflow.security;

import io.github.stellhub.stellflow.server.api.RequestContext;

/**
 * 默认放行授权器。
 */
public class AllowAllAuthorizer implements Authorizer {

    @Override
    public boolean authorize(RequestContext requestContext, String principal) {
        return true;
    }
}
