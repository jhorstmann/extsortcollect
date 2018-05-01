package net.jhorstmann.extsortcollect;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ExternalSortCollectors {


    public static <T> ConfigurationBuilder<T> configuration(Serializer<T> serializer) {
        return new ConfigurationBuilder<T>(serializer);
    }

    public static <T extends Comparable<T>> Collector<T, ?, Stream<T>> externalSort(Serializer<T> serializer) {
        return externalSort(serializer, Comparator.naturalOrder());
    }

    public static <T> Collector<T, ?, Stream<T>> externalSort(Serializer<T> serializer, Comparator<T> comparator) {
        return externalSort(configuration(serializer).withComparator(comparator).build());

    }

    public static <T> Collector<T, ?, Stream<T>> externalSort(Configuration<T> configuration) {
        return Collector.of(supplier(configuration), Accumulator::add, Accumulator::combine, Accumulator::finish, Collector.Characteristics.UNORDERED);
    }

    public static <T> Stream<T> stream(Configuration<T> configuration, Path path) {
        FileSpliterator<T> spliterator = new FileSpliterator<>(path,
                configuration.getSerializer(),
                configuration.getMaxRecordSize(),
                configuration.getWriteBufferSize());
        return StreamSupport.stream(spliterator, false)
                .onClose(spliterator::close);
    }

    public static <T> Stream<T> sorted(Configuration<T> configuration, Path path) {
        FileSpliterator<T> spliterator = new FileSpliterator<>(path,
                configuration.getSerializer(),
                configuration.getMaxRecordSize(),
                configuration.getWriteBufferSize());
        return StreamSupport.stream(spliterator, false)
                .collect(externalSort(configuration));
    }


    private static <T> Supplier<Accumulator<T>> supplier(Configuration<T> configuration) {
        return () -> new Accumulator<>(configuration.getSerializer(),
                configuration.getComparator(),
                configuration.getMaxRecordSize(),
                configuration.getWriteBufferSize(),
                configuration.getInternalSortMaxItems());
    }

    public interface Serializer<T> {
        void write(ByteBuffer buffer, T data);
        T read(ByteBuffer in);
    }

    public static class ConfigurationBuilder<T> {
        static final int DEFAULT_MAX_RECORD_SIZE = 4096;
        static final int DEFAULT_WRITE_BUFFER_SIZE = 16 * DEFAULT_MAX_RECORD_SIZE;
        static final int DEFAULT_INTERNAL_SORT_MAX_ITEMS = 20_000;

        private final Serializer<T> serializer;
        private Comparator<T> comparator;
        private int maxRecordSize = DEFAULT_MAX_RECORD_SIZE;
        private int writeBufferSize = DEFAULT_WRITE_BUFFER_SIZE;
        private int internalSortMaxItems = DEFAULT_INTERNAL_SORT_MAX_ITEMS;

        ConfigurationBuilder(Serializer<T> serializer) {
            this.serializer = serializer;
        }

        public ConfigurationBuilder<T> withComparator(Comparator<T> comparator) {
            this.comparator = comparator;
            return this;
        }

        public ConfigurationBuilder<T> withMaxRecordSize(int maxRecordSize) {
            this.maxRecordSize = maxRecordSize;
            return this;
        }

        public ConfigurationBuilder<T> withWriteBufferSize(int writeBufferSize) {
            this.writeBufferSize = writeBufferSize;
            return this;
        }

        public ConfigurationBuilder<T> withInternalSortMaxItems(int internalSortMaxItems) {
            this.internalSortMaxItems = internalSortMaxItems;
            return this;
        }

        public Configuration<T> build() {
            Comparator<T> comparator = this.comparator;
            if (comparator == null) {
                comparator = naturalOrder();
            }
            if (maxRecordSize > writeBufferSize) {
                throw new IllegalArgumentException("record size must not be larger than write buffer");
            }

            return new Configuration<>(serializer, comparator, maxRecordSize, writeBufferSize, internalSortMaxItems);
        }

        @SuppressWarnings("unchecked")
        private Comparator<T> naturalOrder() {
            return (Comparator<T>)Comparator.naturalOrder();
        }
    }

    public static class Configuration<T> {
        private final Serializer<T> serializer;
        private final Comparator<T> comparator;
        private final int maxRecordSize;
        private final int writeBufferSize;
        private final int internalSortMaxItems;

        Configuration(Serializer<T> serializer, Comparator<T> comparator, int maxRecordSize, int writeBufferSize, int internalSortMaxItems) {
            this.serializer = serializer;
            this.comparator = comparator;
            this.maxRecordSize = maxRecordSize;
            this.writeBufferSize = writeBufferSize;
            this.internalSortMaxItems = internalSortMaxItems;
        }

        public Serializer<T> getSerializer() {
            return serializer;
        }

        public Comparator<T> getComparator() {
            return comparator;
        }

        public int getMaxRecordSize() {
            return maxRecordSize;
        }

        public int getWriteBufferSize() {
            return writeBufferSize;
        }

        public int getInternalSortMaxItems() {
            return internalSortMaxItems;
        }

    }


}
