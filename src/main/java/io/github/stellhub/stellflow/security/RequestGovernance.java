package io.github.stellhub.stellflow.security;

import io.github.stellhub.stellflow.network.protocol.ErrorCode;
import io.github.stellhub.stellflow.server.api.RequestContext;

/**
 * 请求治理链。
 */
public class RequestGovernance {

    private final Authenticator authenticator;
    private final Authorizer authorizer;
    private final ClientQuotaManager quotaManager;

    public RequestGovernance(
            Authenticator authenticator, Authorizer authorizer, ClientQuotaManager quotaManager) {
        this.authenticator = authenticator;
        this.authorizer = authorizer;
        this.quotaManager = quotaManager;
    }

    /**
     * 检查请求是否可继续处理。
     */
    public ErrorCode evaluate(RequestContext requestContext) {
        AuthenticationResult authentication = authenticator.authenticate(requestContext);
        if (!authentication.allowed()) {
            return ErrorCode.AUTHENTICATION_FAILED;
        }
        if (!authorizer.authorize(requestContext, authentication.principal())) {
            return ErrorCode.AUTHORIZATION_FAILED;
        }
        if (!quotaManager.allow(requestContext)) {
            return ErrorCode.THROTTLED;
        }
        return ErrorCode.NONE;
    }

    /**
     * 默认开发治理链。
     */
    public static RequestGovernance allowAll() {
        return new RequestGovernance(
                new AllowAllAuthenticator(), new AllowAllAuthorizer(), new ClientQuotaManager(0));
    }
}
