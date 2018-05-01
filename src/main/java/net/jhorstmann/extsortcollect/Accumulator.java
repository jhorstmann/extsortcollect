package net.jhorstmann.extsortcollect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

class Accumulator<T> {
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

    private final ExternalSortCollectors.Serializer<T> serializer;
    private final Comparator<T> comparator;
    private final int maxRecordSize;
    private final int writeBufferSize;
    private final int internalSortMaxItems;
    private final ArrayList<T> data;
    private final ArrayList<Chunk> chunks;
    private long size;
    private ByteBuffer buffer;
    private FileChannel file;


    Accumulator(ExternalSortCollectors.Serializer<T> serializer, Comparator<T> comparator, int maxRecordSize, int writeBufferSize, int internalSortMaxItems) {
        this.serializer = serializer;
        this.comparator = comparator;
        this.maxRecordSize = maxRecordSize;
        this.writeBufferSize = writeBufferSize;
        this.internalSortMaxItems = internalSortMaxItems;
        this.data = new ArrayList<>(internalSortMaxItems);
        this.chunks = new ArrayList<>();
    }

    void add(T elem) {
        addWithoutSize(elem);
        this.size++;
    }

    private void addWithoutSize(T elem) {
        ArrayList<T> data = this.data;

        data.add(elem);
        if (data.size() >= internalSortMaxItems) {
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
                try (FileChannel other = acc.file) {
                    long offset = file.position();

                    other.transferTo(0, other.size(), this.file);

                    this.chunks.ensureCapacity(this.chunks.size() + acc.chunks.size());
                    for (Chunk chunk : acc.chunks) {
                        this.chunks.add(new Chunk(chunk.offset + offset, chunk.length));
                    }

                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            acc.file = null;
            acc.buffer = null;
        }
        for (T datum : acc.data) {
            addWithoutSize(datum);
        }

        this.size += acc.size;

        return this;
    }

    Stream<T> finish() {
        ArrayList<T> data = this.data;

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

    private PriorityQueue<ReadableChunk<T>> makeQueue(List<Chunk> chunks) throws IOException {
        PriorityQueue<ReadableChunk<T>> queue = new PriorityQueue<>();
        MappedByteBuffer buffer = file.map(FileChannel.MapMode.READ_ONLY, 0, file.size());
        // TODO: create multiple mappings if file to large for single ByteBuffer
        for (int i = 0; i < chunks.size(); i++) {
            Chunk chunk = chunks.get(i);
            ByteBuffer view = buffer.slice();
            view.position(Math.toIntExact(chunk.getOffset()));
            view.limit(Math.toIntExact(chunk.getOffset() + chunk.getLength()));
            queue.add(new ReadableChunk<>(serializer, comparator, view, i));
        }
        return queue;
    }

    private Stream<T> mergedStream() throws IOException {

        if (LOG.isDebugEnabled()) {
            LongSummaryStatistics summary = chunks.stream().mapToLong(Chunk::getLength).summaryStatistics();
            LOG.debug("Merging [{}] chunks with avg size [{}KiB], average record size [{} bytes]",
                    summary.getCount(), (long)Math.ceil(summary.getAverage()/1024), (long)Math.ceil((double)summary.getSum()/size));
        }

        MergeSpliterator<T> spliterator;

        try (FileChannel file = this.file) {
            PriorityQueue<ReadableChunk<T>> queue = makeQueue(this.chunks);

            spliterator = new MergeSpliterator<>(comparator, queue, size);

            this.buffer = null;
            this.file = null;
        }

        // TODO: Unmap buffer on close using reflection and DirectByteBuffer#cleaner()

        return StreamSupport.stream(spliterator, false)
                .onClose(spliterator::close);
    }

    private static FileChannel createTempFile() throws IOException {
        Path tempFile = Files.createTempFile("extsort_", ".tmp");
        return FileChannel.open(tempFile, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.DELETE_ON_CLOSE);
    }

    private void writeSortedBuffer() throws IOException {
        ByteBuffer buffer = this.buffer;
        FileChannel file = this.file;
        ArrayList<Chunk> chunks = this.chunks;

        if (file == null) {
            this.file = file = createTempFile();
        }
        if (buffer == null) {
            this.buffer = buffer = ByteBuffer.allocate(writeBufferSize);
        }
        long offset = file.position();
        for (T datum : data) {
            serializer.write(buffer, datum);
            if (buffer.remaining() < maxRecordSize) {
                buffer.flip();
                file.write(buffer);
                buffer.clear();
            }
        }
        // write remaining data to file
        if (buffer.position() > 0) {
            buffer.flip();
            file.write(buffer);
            buffer.clear();
        }
        long length = file.position() - offset;
        chunks.add(new Chunk(offset, length));
    }

}

