package net.jhorstmann.extsortcollect;

import java.nio.ByteBuffer;

class DataSerializer implements ExternalSortCollectors.Serializer<Data> {

    @Override
    public void write(ByteBuffer buffer, Data data) {
        buffer.putInt(data.getId());
        String key = data.getKey();
        buffer.putInt(key.length());
        for (int i = 0; i < key.length(); i++) {
            buffer.putChar(key.charAt(i));
        }
        String payload = data.getPayload();
        buffer.putInt(payload.length());
        for (int i = 0; i < payload.length(); i++) {
            buffer.putChar(payload.charAt(i));
        }
    }

    @Override
    public Data read(ByteBuffer in) {
        int id = in.getInt();
        if (id <= 0) {
            throw new IllegalStateException("invalid id " + id);
        }
        int keylen = in.getInt();
        if (keylen <= 0) {
            throw new IllegalStateException("negative length " + keylen);
        }
        char[] key = new char[keylen];
        for (int i = 0; i < keylen; i++) {
            key[i] = in.getChar();
            if (key[i] < '0' || key[i] > '9') {
                throw new IllegalStateException("not a digit " + key[i]);
            }
        }
        int payloadlen = in.getInt();
        if (payloadlen <= 0) {
            throw new IllegalStateException("negative length " + keylen);
        }
        char[] payload = new char[payloadlen];
        for (int i = 0; i < payloadlen; i++) {
            payload[i] = in.getChar();
            if (payload[i] < '0' || payload[i] > '9') {
                throw new IllegalStateException("not a digit " + payload[i]);
            }
        }
        return new Data(id, new String(key), new String(payload));
    }
}
