package io.github.stellhub.stellflow.network.transport;

import io.github.stellhub.stellflow.network.protocol.AbortedTransaction;
import io.github.stellhub.stellflow.network.protocol.FetchPartitionResponse;
import io.github.stellhub.stellflow.network.protocol.FetchResponseBody;
import io.github.stellhub.stellflow.network.protocol.FetchTopicResponse;
import io.github.stellhub.stellflow.network.protocol.HeaderCodec;
import io.github.stellhub.stellflow.network.protocol.ProtocolCodecRegistry;
import io.github.stellhub.stellflow.network.protocol.ProtocolSerde;
import io.github.stellhub.stellflow.server.api.FetchRecordsFileRegion;
import io.github.stellhub.stellflow.server.api.ResponseContext;
import io.github.stellhub.stellflow.server.api.ZeroCopyFileRegion;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;
import io.netty.handler.codec.MessageToMessageEncoder;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;

/**
 * 响应编码器。
 */
@RequiredArgsConstructor
public class ResponseEncoder extends MessageToMessageEncoder<ResponseContext> {

    private final ProtocolCodecRegistry protocolCodecRegistry;

    /**
     * 编码统一响应头和响应体。
     */
    @Override
    protected void encode(ChannelHandlerContext ctx, ResponseContext msg, List<Object> out) {
        if (shouldEncodeFetchZeroCopyRecords(msg)) {
            encodeFetchZeroCopyRecords(ctx, msg, out);
            return;
        }
        ByteBuf payload = ctx.alloc().buffer();
        try {
            HeaderCodec.encodeResponseHeader(payload, msg.getResponseHeader());
            protocolCodecRegistry.encodeResponseBody(
                    msg.getApiKey(), msg.getApiVersion(), msg.getResponseBody(), payload);

            long zeroCopyBytes =
                    msg.getZeroCopyFileRegions().stream().mapToLong(ZeroCopyFileRegion::count).sum();
            long frameLength = payload.readableBytes() + zeroCopyBytes;
            if (frameLength > Integer.MAX_VALUE) {
                throw new IllegalStateException("Response frame is too large for length prefix: " + frameLength);
            }
            ByteBuf frame = ctx.alloc().buffer(Integer.BYTES + payload.readableBytes());
            frame.writeInt((int) frameLength);
            frame.writeBytes(payload);
            out.add(frame);
            for (ZeroCopyFileRegion region : msg.getZeroCopyFileRegions()) {
                out.add(toDefaultFileRegion(region));
            }
        } finally {
            payload.release();
        }
    }

    private boolean shouldEncodeFetchZeroCopyRecords(ResponseContext msg) {
        return msg.getResponseBody() instanceof FetchResponseBody
                && !msg.getFetchRecordsFileRegions().isEmpty();
    }

    private void encodeFetchZeroCopyRecords(ChannelHandlerContext ctx, ResponseContext msg, List<Object> out) {
        List<Object> parts = new ArrayList<>();
        FetchZeroCopyPartWriter writer = new FetchZeroCopyPartWriter(ctx, parts);
        try {
            HeaderCodec.encodeResponseHeader(writer.buffer(), msg.getResponseHeader());
            encodeFetchBodyParts(
                    (FetchResponseBody) msg.getResponseBody(),
                    new ArrayList<>(msg.getFetchRecordsFileRegions()),
                    writer);
            writer.finish();
            writeFramedParts(ctx, parts, out);
        } finally {
            writer.releaseCurrent();
            if (out.isEmpty()) {
                releaseParts(parts);
            }
        }
    }

    private void encodeFetchBodyParts(
            FetchResponseBody body,
            List<FetchRecordsFileRegion> recordRegions,
            FetchZeroCopyPartWriter writer) {
        ByteBuf current = writer.buffer();
        current.writeInt(body.sessionId());
        current.writeInt(body.responses().size());
        for (FetchTopicResponse topicResponse : body.responses()) {
            ProtocolSerde.writeNullableString(current, topicResponse.topic());
            current.writeInt(topicResponse.partitions().size());
            for (FetchPartitionResponse partitionResponse : topicResponse.partitions()) {
                encodeFetchPartitionPrefix(current, partitionResponse);
                FetchRecordsFileRegion region =
                        removeFirstMatching(recordRegions, topicResponse.topic(), partitionResponse.partition());
                if (region == null) {
                    ProtocolSerde.writeBytes(current, partitionResponse.records());
                    continue;
                }
                current.writeInt(region.readableBytes());
                writer.flush();
                current = writer.buffer();
                for (ZeroCopyFileRegion fileRegion : region.fileRegions()) {
                    if (fileRegion.count() > 0) {
                        writer.add(fileRegion);
                    }
                }
            }
        }
    }

    private void encodeFetchPartitionPrefix(ByteBuf current, FetchPartitionResponse partitionResponse) {
        current.writeInt(partitionResponse.partition());
        current.writeShort(partitionResponse.errorCode().code());
        current.writeLong(partitionResponse.highWatermark());
        current.writeLong(partitionResponse.logStartOffset());
        current.writeLong(partitionResponse.lastStableOffset());
        current.writeInt(partitionResponse.abortedTransactions().size());
        for (AbortedTransaction abortedTransaction : partitionResponse.abortedTransactions()) {
            current.writeLong(abortedTransaction.producerId());
            current.writeLong(abortedTransaction.firstOffset());
        }
    }

    private FetchRecordsFileRegion removeFirstMatching(
            List<FetchRecordsFileRegion> recordRegions, String topic, int partition) {
        for (int index = 0; index < recordRegions.size(); index++) {
            FetchRecordsFileRegion candidate = recordRegions.get(index);
            if (candidate.matches(topic, partition)) {
                return recordRegions.remove(index);
            }
        }
        return null;
    }

    private void writeFramedParts(ChannelHandlerContext ctx, List<Object> parts, List<Object> out) {
        long frameLength = frameLength(parts);
        if (frameLength > Integer.MAX_VALUE) {
            throw new IllegalStateException("Response frame is too large for length prefix: " + frameLength);
        }
        boolean wroteFirstBuffer = false;
        for (Object part : parts) {
            if (part instanceof ByteBuf buffer) {
                if (!wroteFirstBuffer) {
                    ByteBuf frame = ctx.alloc().buffer(Integer.BYTES + buffer.readableBytes());
                    frame.writeInt((int) frameLength);
                    frame.writeBytes(buffer, buffer.readerIndex(), buffer.readableBytes());
                    buffer.release();
                    out.add(frame);
                    wroteFirstBuffer = true;
                } else {
                    out.add(buffer);
                }
                continue;
            }
            if (part instanceof ZeroCopyFileRegion region) {
                out.add(toDefaultFileRegion(region));
            }
        }
        if (!wroteFirstBuffer) {
            ByteBuf frame = ctx.alloc().buffer(Integer.BYTES);
            frame.writeInt((int) frameLength);
            out.add(frame);
        }
    }

    private long frameLength(List<Object> parts) {
        long frameLength = 0;
        for (Object part : parts) {
            if (part instanceof ByteBuf buffer) {
                frameLength += buffer.readableBytes();
            } else if (part instanceof ZeroCopyFileRegion region) {
                frameLength += region.count();
            }
        }
        return frameLength;
    }

    private void releaseParts(List<Object> parts) {
        for (Object part : parts) {
            if (part instanceof ByteBuf buffer) {
                buffer.release();
            }
        }
    }

    private DefaultFileRegion toDefaultFileRegion(ZeroCopyFileRegion region) {
        return new DefaultFileRegion(region.file().toFile(), region.position(), region.count());
    }

    private static final class FetchZeroCopyPartWriter {

        private final ChannelHandlerContext ctx;
        private final List<Object> parts;
        private ByteBuf current;

        private FetchZeroCopyPartWriter(ChannelHandlerContext ctx, List<Object> parts) {
            this.ctx = ctx;
            this.parts = parts;
            this.current = ctx.alloc().buffer();
        }

        private ByteBuf buffer() {
            return current;
        }

        private void flush() {
            if (current.readableBytes() > 0) {
                parts.add(current);
                current = ctx.alloc().buffer();
            }
        }

        private void add(ZeroCopyFileRegion region) {
            parts.add(region);
        }

        private void finish() {
            if (current == null) {
                return;
            }
            if (current.readableBytes() > 0) {
                parts.add(current);
                current = null;
                return;
            }
            current.release();
            current = null;
        }

        private void releaseCurrent() {
            if (current != null) {
                current.release();
                current = null;
            }
        }
    }
}
