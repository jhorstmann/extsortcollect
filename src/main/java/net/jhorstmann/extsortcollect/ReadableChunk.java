package net.jhorstmann.extsortcollect;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Comparator;
import java.util.Iterator;

class ReadableChunk<T> implements Comparable<ReadableChunk<T>>, Iterator<T>, Closeable {
    private final ExternalSortCollectors.Serializer<T> serializer;
    private final Comparator<T> comparator;
    private final int stableOrder;
    private ByteBuffer buffer;
    private T data;

    ReadableChunk(ExternalSortCollectors.Serializer<T> serializer, Comparator<T> comparator, ByteBuffer buffer, int stableOrder) throws IOException {
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

    int writeCurrentElementTo(WritableByteChannel dst) throws IOException {
        int position = buffer.position();
        int limit = buffer.limit();
        buffer.reset();
        buffer.limit(position);
        int bytesWritten = dst.write(buffer);
        buffer.limit(limit);
        return bytesWritten;
    }

    T current() {
        if (data == null) {
            buffer.mark();
            data = serializer.read(buffer);
        }
        return data;
    }

    @Override
    public boolean hasNext() {
        return data != null || !isEmpty();
    }

    @Override
    public T next() {
        T current = current();
        data = null;
        return current;
    }

    boolean isEmpty() {
        return buffer.remaining() == 0;
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
