package io.github.stellhub.stellflow.network.transport;

import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * 帧长度解码器。
 */
public class FrameLengthDecoder extends LengthFieldBasedFrameDecoder {

    /**
     * 创建长度前缀解码器。
     */
    public FrameLengthDecoder(int maxFrameLength) {
        super(maxFrameLength, 0, 4, 0, 4);
    }
}
