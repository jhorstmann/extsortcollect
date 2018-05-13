package net.jhorstmann.extsortcollect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

class Accumulator<T> implements Closeable  {
    private static final Logger LOG = LoggerFactory.getLogger(ExternalSortCollectors.class);

    static class Chunk {
        private final long offset;
        private final long length;

        Chunk(long offset, long length) {
            this.offset = offset;
            this.length = length;
        }

        long getOffset() {
            return offset;
        }

        long getLength() {
            return length;
        }

        @Override
        public String toString() {
            return String.format("[%d, %d)", offset, offset+length);
        }
    }

    static class Elements<T> {
        private final Object[] data;
        private final boolean parallel;
        private int size;

        Elements(int capacity, boolean parallel) {
            this.data = new Object[capacity];
            this.parallel = parallel;
            this.size = 0;
        }

        @SuppressWarnings("unchecked")
        void sort(Comparator<T> comparator) {
            long t1 = System.currentTimeMillis();

            if (parallel) {
                Arrays.parallelSort(data, 0, size, (Comparator<Object>) comparator);
            } else {
                Arrays.sort(data, 0, size, (Comparator<Object>) comparator);
            }

            if (LOG.isTraceEnabled()) {
                long t2 = System.currentTimeMillis();
                LOG.trace("Sorted [{}] elements in [{}ms] [{}]", size, t2 - t1, parallel ?  "parallel" : "");
            }
        }

        void add(T elem) {
            data[size++] = elem;
        }

        @SuppressWarnings("unchecked")
        T get(int idx) {
            return (T)data[idx];
        }

        @SuppressWarnings("unchecked")
        T getAndClear(int idx) {
            T elem = (T)data[idx];
            data[idx] = null;
            return elem;
        }

        int size() {
            return size;
        }

        boolean isFull() {
            return size == data.length;
        }

        void clear() {
            size = 0;
        }

        @SuppressWarnings("unchecked")
        Stream<T> stream() {
            return (Stream<T>)Arrays.stream(data, 0, size);
        }
    }

    private final ExternalSortCollectors.Serializer<T> serializer;
    private final Comparator<T> comparator;
    private final int maxRecordSize;
    private final int writeBufferSize;
    private final Elements<T> data;
    private final ArrayList<Chunk> chunks;
    private long totalSize;
    private ByteBuffer buffer;
    private ByteBuffer directBuffer;
    private FileChannel file;


    Accumulator(ExternalSortCollectors.Serializer<T> serializer, Comparator<T> comparator, int maxRecordSize, int writeBufferSize, int internalSortMaxItems, boolean parallelSort) {
        this.serializer = serializer;
        this.comparator = comparator;
        this.maxRecordSize = maxRecordSize;
        this.writeBufferSize = writeBufferSize;
        this.data = new Elements<>(internalSortMaxItems, parallelSort);
        this.chunks = new ArrayList<>();
    }

    void add(T elem) {
        addWithoutSize(elem);
        this.totalSize++;
    }

    private void addWithoutSize(T elem) {
        Elements<T> data = this.data;

        data.add(elem);
        if (data.isFull()) {
            data.sort(comparator);

            try {
                writeSortedBuffer();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            data.clear();
        }
    }

    Accumulator<T> combine(Accumulator<T> acc) {
        if (acc.file != null) {
            if (this.file == null) {
                this.file = acc.file;
                this.buffer = acc.buffer;
            } else {
                long t1 = System.currentTimeMillis();

                try {
                    try (Closeable closeable = acc) {
                        FileChannel source = acc.file;
                        long offset = file.position();

                        source.transferTo(0, source.size(), this.file);

                        this.chunks.ensureCapacity(this.chunks.size() + acc.chunks.size());
                        for (Chunk chunk : acc.chunks) {
                            this.chunks.add(new Chunk(chunk.offset + offset, chunk.length));
                        }
                    }
                } catch (IOException e) {
                    try {
                        close();
                    } catch (Throwable t2) {
                        e.addSuppressed(t2);
                    }
                    throw new UncheckedIOException(e);
                } catch (Throwable t) {
                    try {
                        close();
                    } catch (Throwable t2) {
                        t.addSuppressed(t2);
                    }
                    throw t;
                }

                if (LOG.isTraceEnabled()) {
                    long t2 = System.currentTimeMillis();
                    LOG.trace("Combined chunks in [{}ms]", (t2 - t1));
                }
            }
        }
        for (int i = 0; i < acc.data.size(); i++) {
            T elem = acc.data.get(i);
            addWithoutSize(elem);
        }

        this.totalSize += acc.totalSize;

        return this;
    }

    Stream<T> finish() {
        Elements<T> data = this.data;

        if (data.size() > 0) {
            data.sort(comparator);
        }
        if (file == null) {
            return data.stream();
        } else {
            try {
                if (data.size() > 0) {
                    writeSortedBuffer();
                }
                return mergedStream();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        this.buffer = null;

        ByteBuffer directBuffer = this.directBuffer;
        if (directBuffer != null) {
            Cleaner.clean(directBuffer);
            this.directBuffer = null;
        }

        if (file != null) {
            this.file.close();
            this.file = null;
        }
    }

    private PriorityQueue<ReadableChunk<T>> makeQueue(List<Chunk> chunks) throws IOException {
        PriorityQueue<ReadableChunk<T>> queue = new PriorityQueue<>();
        MappedByteBuffer buffer = file.map(FileChannel.MapMode.READ_ONLY, 0, file.size());
        // TODO: create multiple mappings if file to large for single ByteBuffer
        AtomicInteger referenceCount = new AtomicInteger(0);
        for (int i = 0; i < chunks.size(); i++) {
            Chunk chunk = chunks.get(i);

            ByteBuffer view = buffer.slice();
            view.position(Math.toIntExact(chunk.getOffset()));
            view.limit(Math.toIntExact(chunk.getOffset() + chunk.getLength()));

            Cleaner cleaner = new Cleaner(referenceCount, buffer);

            queue.add(new ReadableChunk<>(serializer, comparator, view, cleaner, i));
        }
        return queue;
    }

    private Stream<T> mergedStream() throws IOException {
        try (Closeable closeable = this) {
            if (LOG.isDebugEnabled()) {
                LongSummaryStatistics summary = chunks.stream().mapToLong(Chunk::getLength).summaryStatistics();
                LOG.debug("Merging [{}] chunks with avg size [{}KiB], average record size [{} bytes]",
                        summary.getCount(), (long)Math.ceil(summary.getAverage()/1024), (long)Math.ceil((double)summary.getSum()/ totalSize));
            }

            PriorityQueue<ReadableChunk<T>> queue = makeQueue(this.chunks);

            ChunkMergeSpliterator<T> spliterator = new ChunkMergeSpliterator<>(comparator, queue, totalSize);

            return StreamSupport.stream(spliterator, false)
                    .onClose(spliterator::close);
        }
    }

    private static FileChannel createTempFile() throws IOException {
        Path tempFile = Files.createTempFile("extsort_", ".tmp");
        return FileChannel.open(tempFile, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.DELETE_ON_CLOSE);
    }

    private void writeSortedBuffer() throws IOException {
        ByteBuffer buffer = this.buffer;
        ByteBuffer directBuffer = this.directBuffer;
        FileChannel file = this.file;
        ArrayList<Chunk> chunks = this.chunks;

        try {
            if (file == null) {
                this.file = file = createTempFile();
            }
            if (buffer == null) {
                this.buffer = buffer = ByteBuffer.allocate(writeBufferSize);
                this.directBuffer = directBuffer = ByteBuffer.allocateDirect(writeBufferSize);
            }

            long t1 = System.currentTimeMillis();
            int blocks = 0;
            long offset = file.position();
            for (int i = 0; i < data.size(); i++) {
                T elem = data.getAndClear(i);
                serializer.write(buffer, elem);
                if (buffer.remaining() < maxRecordSize) {
                    buffer.flip();

                    directBuffer.clear();
                    directBuffer.put(buffer);
                    directBuffer.flip();

                    file.write(directBuffer);

                    buffer.clear();
                    blocks++;
                }
            }
            // write remaining data to file
            if (buffer.position() > 0) {
                buffer.flip();

                directBuffer.clear();
                directBuffer.put(buffer);
                directBuffer.flip();

                file.write(directBuffer);

                buffer.clear();
                blocks++;
            }
            long length = file.position() - offset;

            if (LOG.isTraceEnabled()) {
                long t2 = System.currentTimeMillis();
                LOG.trace("Wrote chunk with [{}] blocks in [{}ms]", blocks, t2 - t1);
            }

            chunks.add(new Chunk(offset, length));
        } catch (Throwable t) {
            try {
                close();
            } catch (Throwable t2) {
                t.addSuppressed(t2);
            }
            throw t;
        }
    }

}

