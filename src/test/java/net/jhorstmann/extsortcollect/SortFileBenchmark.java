package net.jhorstmann.extsortcollect;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class SortFileBenchmark {
    public static void main(String[] args) throws IOException {

        DataSerializer serializer = new DataSerializer();
        Comparator<Data> comparator = Comparator.comparing(Data::getId);
        Path path = Paths.get("src/test/resources/random.data");
        FileChannel file = FileChannel.open(path, StandardOpenOption.READ);

        Collector<Data, ?, Stream<Data>> collector = ExternalSortCollectors.externalSort(serializer, comparator);

        try (Stream<Data> stream = StreamSupport.stream(new FileSpliterator(file, serializer), false)
        .onClose(() -> {
            try {
                file.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        })) {
            List<Data> list = stream.collect(collector)
                    .skip(1000)
                    .limit(100)
                    .collect(Collectors.toList());

            System.out.println(list);
        }
    }

    static class FileSpliterator implements Spliterator<Data> {
        private final FileChannel file;
        private final ExternalSortCollectors.Serializer<Data> serializer;
        private final ByteBuffer buffer;

        FileSpliterator(Path path, ExternalSortCollectors.Serializer<Data> serializer) throws IOException {
            this(FileChannel.open(path, StandardOpenOption.READ), serializer);
        }

        FileSpliterator(FileChannel file, ExternalSortCollectors.Serializer<Data> serializer) {
            this.file = file;
            this.serializer = serializer;
            this.buffer = ByteBuffer.allocate(4096 * 16);
            this.buffer.limit(0);
        }

        @Override
        public boolean tryAdvance(Consumer<? super Data> action) {
            try {
                long fileRemaining = file.size() - file.position();
                if (fileRemaining == 0 && buffer.remaining() == 0) {
                    return false;
                }

                if (buffer.remaining() < 4096 && fileRemaining > 0) {
                    buffer.compact();
                    file.read(buffer);
                    buffer.flip();
                }

                Data data = serializer.read(buffer);
                action.accept(data);

                return true;

            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
/*
        @Override
        public void forEachRemaining(Consumer<? super Data> action) {
            buffer.compact();

            try {
                while (file.read(buffer) != -1) {
                    buffer.flip();
                    while (buffer.remaining() >= 4096) {
                        Data data = serializer.read(buffer);
                        action.accept(data);
                    }
                    buffer.compact();
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        */

        @Override
        public Spliterator<Data> trySplit() {
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
    }
}
