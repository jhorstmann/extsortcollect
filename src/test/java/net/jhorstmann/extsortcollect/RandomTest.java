package net.jhorstmann.extsortcollect;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

class RandomTest {

    static class Data {
        private final int id;
        private final String value;

        Data(int id, String value) {
            this.id = id;
            this.value = value;
        }

        int getId() {
            return id;
        }

        String getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Data data = (Data) o;

            if (id != data.id) return false;
            return value.equals(data.value);
        }

        @Override
        public int hashCode() {
            return id;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    static class DataSerializer implements ExternalSortCollectors.Serializer<Data> {

        @Override
        public void write(ByteBuffer buffer, Data data) {
            buffer.putInt(data.getId());
            String value = data.getValue();
            buffer.putInt(value.length());
            for (int i = 0; i < value.length(); i++) {
                buffer.putChar(value.charAt(i));
            }
        }

        @Override
        public Data read(ByteBuffer in) {
            int id = in.getInt();
            int len = in.getInt();
            char[] value = new char[len];
            for (int i = 0; i < len; i++) {
                value[i] = in.getChar();
            }
            return new Data(id, new String(value));
        }
    }

    @ParameterizedTest
    @MethodSource("randomNumbers")
    void shouldSortSmallArraysInMemory(List<Data> data) {

        Comparator<Data> comparator = Comparator.comparing(Data::getId);

        ExternalSortCollectors.Configuration<Data> configuration = ExternalSortCollectors.configuration(new DataSerializer())
                .withComparator(comparator)
                .withInternalSortMaxItems(50)
                .build();

        sortAndCompare(data, configuration);

    }

    @ParameterizedTest
    @MethodSource("randomNumbers")
    void shouldSortMoreThanMaxItemsExternally(List<Data> data) {

        Comparator<Data> comparator = Comparator.comparing(Data::getId);

        ExternalSortCollectors.Configuration<Data> configuration = ExternalSortCollectors.configuration(new DataSerializer())
                .withComparator(comparator)
                .withInternalSortMaxItems(4)
                .build();

        sortAndCompare(data, configuration);
    }

    @ParameterizedTest
    @MethodSource("randomNumbers")
    void shouldSortWithSmallWriteBuffer(List<Data> data) {

        Comparator<Data> comparator = Comparator.comparing(Data::getId);

        ExternalSortCollectors.Configuration<Data> configuration = ExternalSortCollectors.configuration(new DataSerializer())
                .withComparator(comparator)
                .withInternalSortMaxItems(10)
                .withMaxRecordSize(50)
                .withWriteBufferSize(100)
                .build();

        sortAndCompare(data, configuration);
    }

    @ParameterizedTest
    @MethodSource("randomNumbers")
    void shouldSortParallelStream(List<Data> data) {

        Comparator<Data> comparator = Comparator.comparing(Data::getId);

        ExternalSortCollectors.Configuration<Data> configuration = ExternalSortCollectors.configuration(new DataSerializer())
                .withComparator(comparator)
                .withInternalSortMaxItems(10)
                .withMaxRecordSize(50)
                .withWriteBufferSize(100)
                .build();

        sortAndCompare(data, configuration, true);
    }

    private void sortAndCompare(List<Data> data, ExternalSortCollectors.Configuration<Data> configuration) {
        sortAndCompare(data, configuration, false);
    }


    private void sortAndCompare(List<Data> data, ExternalSortCollectors.Configuration<Data> configuration, boolean parallelStream) {
        Stream<Data> stream = data.stream();
        if (parallelStream) {
            stream = stream.parallel();
        }
        try (Stream<Data> sortedStream = stream.collect(ExternalSortCollectors.externalSort(configuration))) {

            Data[] externallySorted = sortedStream.toArray(Data[]::new);

            Data[] internallySorted = data.stream()
                    .sorted(configuration.getComparator())
                    .toArray(Data[]::new);

            //Assertions.assertArrayEquals(internallySorted, externallySorted);
            assertThat(externallySorted).containsExactly(internallySorted);
        }
    }

    private static Stream<List<Data>> randomNumbers() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        return IntStream.of(1, 2, 3, 5, 10, 20, 100, 101, 10_000)
                .mapToObj(size -> random.ints(size, 0, 10000)
                        .mapToObj(i -> new Data(i, String.valueOf(i * 31)))
                        .collect(toList()));
    }

}