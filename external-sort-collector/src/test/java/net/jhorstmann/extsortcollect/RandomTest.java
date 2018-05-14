package net.jhorstmann.extsortcollect;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

class RandomTest {

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
                .mapToObj(size -> random.ints(size, 1, 100_000)
                        .mapToObj(i -> new Data(i, String.valueOf(i * 31)))
                        .collect(toList()));
    }

}