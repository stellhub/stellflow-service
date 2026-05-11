package io.github.stellhub.stellflow.network.protocol;

/**
 * 空响应体占位对象。
 */
public final class EmptyResponseBody implements ResponseBody {

    public static final EmptyResponseBody INSTANCE = new EmptyResponseBody();

    private EmptyResponseBody() {}
}
