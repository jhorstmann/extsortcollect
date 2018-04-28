package net.jhorstmann.extsortcollect;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class SortFileBenchmark {
    public static void main(String[] args) throws IOException {

        DataSerializer serializer = new DataSerializer();
        Comparator<Data> comparator = Comparator.comparing(Data::getId);
        Path path = Paths.get("data/random.data");

        long t1 = System.currentTimeMillis();

        /*
        dataStream.sequential().reduce((a, b) -> b).ifPresent(
                System.out::println
        );
        */

        /*

        try (Stream<Data> stream = dataStream) {
            List<Data> list = stream
                    .sorted(comparator)
                    .skip(1000)
                    .limit(100)
                    .collect(Collectors.toList());

            System.out.println(list);
        }
        */

        ExternalSortCollectors.Configuration<Data> configuration = ExternalSortCollectors.configuration(serializer)
                .withComparator(comparator)
                .withInternalSortMaxItems(100_000)
                .withMaxRecordSize(1024)
                .withWriteBufferSize(64 * 4096)
                .withMaxNumberOfChunks(1000)
                .build();

        Stream<Data> dataStream = ExternalSortCollectors.stream(configuration, path);


        Collector<Data, ?, Stream<Data>> collector = ExternalSortCollectors.externalSort(configuration);
        try (Stream<Data> stream = dataStream) {
            List<Data> list = stream.collect(collector)
                    .skip(10_000)
                    .limit(100)
                    .collect(Collectors.toList());

            System.out.println(list);
        }

        System.out.println((System.currentTimeMillis()-t1) / 1000.0);
    }

}
