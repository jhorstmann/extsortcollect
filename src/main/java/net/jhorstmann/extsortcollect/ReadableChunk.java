package net.jhorstmann.extsortcollect;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;

class ReadableChunk<T> implements Comparable<ReadableChunk<T>>, Iterator<T>, Closeable {
    private final ExternalSortCollectors.Serializer<T> serializer;
    private final Comparator<T> comparator;
    private final int stableOrder;
    private ByteBuffer buffer;
    private T data;

    ReadableChunk(ExternalSortCollectors.Serializer<T> serializer, Comparator<T> comparator, ByteBuffer buffer, int stableOrder) {
        this.serializer = serializer;
        this.comparator = comparator;
        this.stableOrder = stableOrder;
        this.buffer = buffer;
    }

    @Override
    public void close() {
        // TODO: unmap buffer;
        this.buffer = null;
        this.data = null;
    }

    private T current() {
        ByteBuffer buffer = this.buffer;
        T data = this.data;

        if (data == null) {
            buffer.mark();
            this.data = data = serializer.read(buffer);
        }
        return data;
    }

    @Override
    public boolean hasNext() {
        return data != null || buffer.remaining() > 0;
    }

    @Override
    public T next() {
        T current = current();
        data = null;
        return current;
    }

    @Override
    public int compareTo(ReadableChunk<T> o) {
        T d1 = current();
        T d2 = o.current();

        int res = comparator.compare(d1, d2);
        if (res != 0) {
            return res;
        } else {
            return Integer.compare(stableOrder, o.stableOrder);
        }
    }
}
