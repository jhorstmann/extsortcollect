package net.jhorstmann.extsortcollect.benchmark;

import net.jhorstmann.extsortcollect.ExternalSortCollectors;

import java.nio.ByteBuffer;

class DataSerializer implements ExternalSortCollectors.Serializer<Data> {

    @Override
    public void write(ByteBuffer buffer, Data data) {
        buffer.putInt(data.getId());
        String key = data.getPayload();
        buffer.putInt(key.length());
        for (int i = 0; i < key.length(); i++) {
            buffer.putChar(key.charAt(i));
        }
    }

    @Override
    public Data read(ByteBuffer in) {
        int id = in.getInt();
        if (id <= 0) {
            throw new IllegalStateException("invalid id " + id);
        }
        int payloadlen = in.getInt();
        if (payloadlen <= 0) {
            throw new IllegalStateException("negative length " + payloadlen);
        }
        char[] payload = new char[payloadlen];
        for (int i = 0; i < payloadlen; i++) {
            payload[i] = in.getChar();
            if (payload[i] < '0' || payload[i] > '9') {
                throw new IllegalStateException("not a digit " + payload[i]);
            }
        }
        return new Data(id, new String(payload));
    }
}
