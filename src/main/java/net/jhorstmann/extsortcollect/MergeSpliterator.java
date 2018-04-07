package net.jhorstmann.extsortcollect;

import java.io.Closeable;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Spliterator;
import java.util.function.Consumer;

class MergeSpliterator<T> implements Spliterator<T>, Closeable {
    private final Comparator<T> comparator;
    private final PriorityQueue<ReadableChunk<T>> chunks;
    private final long size;

    MergeSpliterator(Comparator<T> comparator, PriorityQueue<ReadableChunk<T>> chunks, long size) {
        this.comparator = comparator;
        this.chunks = chunks;
        this.size = size;
    }

    @Override
    public void close() {
        for (ReadableChunk<T> chunk : chunks) {
            chunk.close();
        }
        chunks.clear();
    }

    @Override
    public boolean tryAdvance(Consumer<? super T> action) {
        ReadableChunk<T> chunk = chunks.remove();

        T data = chunk.next();
        action.accept(data);

        if (chunk.hasNext()) {
            chunks.add(chunk);
        } else {
            chunk.close();
        }

        return !chunks.isEmpty();
    }

    @Override
    public Spliterator<T> trySplit() {
        return null;
    }

    @Override
    public int characteristics() {
        return Spliterator.IMMUTABLE | Spliterator.NONNULL | Spliterator.SORTED;
    }

    @Override
    public Comparator<? super T> getComparator() {
        return comparator;
    }

    @Override
    public long estimateSize() {
        return size;
    }

    @Override
    public long getExactSizeIfKnown() {
        return size;
    }
}
