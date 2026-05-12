package io.github.stellhub.stellflow.security;

import io.github.stellhub.stellflow.server.api.RequestContext;

/**
 * ACL 授权器。
 */
public interface Authorizer {

    /**
     * 判断请求是否允许访问。
     */
    boolean authorize(RequestContext requestContext, String principal);
}
