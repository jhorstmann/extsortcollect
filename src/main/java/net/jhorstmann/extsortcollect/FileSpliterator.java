package net.jhorstmann.extsortcollect;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Spliterator;
import java.util.function.Consumer;

class FileSpliterator<T> implements Spliterator<T>, Closeable {
    private final FileChannel file;
    private final ExternalSortCollectors.Serializer<T> serializer;
    private final ByteBuffer buffer;
    private final int maxRecordSize;

    FileSpliterator(Path path, ExternalSortCollectors.Serializer<T> serializer, int maxRecordSize, int bufferSize) {
        this(open(path), serializer, maxRecordSize, bufferSize);
    }

    private static FileChannel open(Path path) {
        try {
            return FileChannel.open(path, StandardOpenOption.READ);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    FileSpliterator(FileChannel file, ExternalSortCollectors.Serializer<T> serializer, int maxRecordSize, int bufferSize) {
        this.file = file;
        this.serializer = serializer;
        this.maxRecordSize = maxRecordSize;
        this.buffer = ByteBuffer.allocate(bufferSize);
        this.buffer.limit(0);
    }

    @Override
    public boolean tryAdvance(Consumer<? super T> action) {
        ByteBuffer buffer = this.buffer;
        FileChannel file = this.file;

        try {
            long fileRemaining = file.size() - file.position();
            if (fileRemaining == 0 && buffer.remaining() == 0) {
                return false;
            }

            if (buffer.remaining() < maxRecordSize && fileRemaining > 0) {
                buffer.compact();
                file.read(buffer);
                buffer.flip();
            }

            T data = serializer.read(buffer);
            action.accept(data);

            return true;

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void forEachRemaining(Consumer<? super T> action) {
        ByteBuffer buffer = this.buffer;
        FileChannel file = this.file;

        buffer.compact();

        try {
            while (file.read(buffer) != -1) {
                buffer.flip();
                while (buffer.remaining() >= maxRecordSize) {
                    T data = serializer.read(buffer);
                    action.accept(data);
                }
                buffer.compact();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Spliterator<T> trySplit() {
        return null;
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return Spliterator.IMMUTABLE | Spliterator.ORDERED | Spliterator.NONNULL;
    }

    @Override
    public void close() {
        try {
            file.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
