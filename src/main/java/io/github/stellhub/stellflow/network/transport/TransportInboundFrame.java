package io.github.stellhub.stellflow.network.transport;

import io.github.stellhub.stellflow.network.protocol.RequestHeader;
import io.netty.buffer.ByteBuf;

/**
 * 头体拆分后的入站帧。
 */
public record TransportInboundFrame(RequestHeader header, ByteBuf bodyBuffer) {}
