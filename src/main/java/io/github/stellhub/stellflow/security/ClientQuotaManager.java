package io.github.stellhub.stellflow.security;

import io.github.stellhub.stellflow.server.api.RequestContext;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 简单客户端配额管理器。
 */
public class ClientQuotaManager {

    private final int maxRequestsPerSecond;
    private final Map<String, WindowCounter> counters = new ConcurrentHashMap<>();

    public ClientQuotaManager(int maxRequestsPerSecond) {
        this.maxRequestsPerSecond = maxRequestsPerSecond;
    }

    /**
     * 判断请求是否命中限流。
     */
    public boolean allow(RequestContext requestContext) {
        if (maxRequestsPerSecond <= 0) {
            return true;
        }
        String key =
                requestContext.getQuotaKey() == null || requestContext.getQuotaKey().isBlank()
                        ? requestContext.getClientId()
                        : requestContext.getQuotaKey();
        if (key == null || key.isBlank()) {
            key = "anonymous";
        }
        return counters.computeIfAbsent(key, ignored -> new WindowCounter()).tryAcquire(maxRequestsPerSecond);
    }

    private static final class WindowCounter {
        private long windowSecond = System.currentTimeMillis() / 1000;
        private int count;

        private synchronized boolean tryAcquire(int limit) {
            long nowSecond = System.currentTimeMillis() / 1000;
            if (nowSecond != windowSecond) {
                windowSecond = nowSecond;
                count = 0;
            }
            if (count >= limit) {
                return false;
            }
            count++;
            return true;
        }
    }
}
