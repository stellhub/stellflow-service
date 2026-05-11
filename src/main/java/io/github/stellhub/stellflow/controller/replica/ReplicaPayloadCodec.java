package io.github.stellhub.stellflow.controller.replica;

import io.github.stellhub.stellflow.storage.log.ReplicaLogEntry;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 副本同步载荷编解码器。
 */
public final class ReplicaPayloadCodec {

    private ReplicaPayloadCodec() {}

    /**
     * 编码复制载荷。
     */
    public static byte[] encode(List<ReplicaLogEntry> entries) {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(byteArrayOutputStream);
            output.writeInt(entries.size());
            for (ReplicaLogEntry entry : entries) {
                output.writeLong(entry.offset());
                output.writeLong(entry.timestamp());
                output.writeInt(entry.leaderEpoch());
                output.writeInt(entry.records().length);
                output.write(entry.records());
            }
            output.flush();
            return byteArrayOutputStream.toByteArray();
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    /**
     * 解码复制载荷。
     */
    public static List<ReplicaLogEntry> decode(byte[] payload) {
        try {
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload));
            int count = input.readInt();
            List<ReplicaLogEntry> entries = new ArrayList<>(Math.max(count, 0));
            for (int index = 0; index < count; index++) {
                long offset = input.readLong();
                long timestamp = input.readLong();
                int leaderEpoch = input.readInt();
                int length = input.readInt();
                byte[] records = input.readNBytes(length);
                entries.add(new ReplicaLogEntry(offset, timestamp, leaderEpoch, records));
            }
            return entries;
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }
}
