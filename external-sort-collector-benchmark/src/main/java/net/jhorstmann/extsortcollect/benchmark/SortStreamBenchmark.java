package net.jhorstmann.extsortcollect.benchmark;

import net.jhorstmann.extsortcollect.ExternalSortCollectors;
import org.geirove.exmeso.CloseableIterator;
import org.geirove.exmeso.ExternalMergeSort;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@BenchmarkMode(Mode.AverageTime)
public class SortStreamBenchmark {

    private static final long SEED = 1234567890L;
    private static final int STREAM_SIZE = 10_000_000;
    private static final int RAND_MAX = 100_000;
    private static final int SKIP = 10_000;
    private static final int LIMIT = 100;

    @State(Scope.Benchmark)
    public static class Input {
        @Param({"1", "5", "10", "20", "50"})
        int payloadRepeat;
    }


    @Benchmark
    public List<Data> exmesoSort(Input input) throws IOException {


        ExternalMergeSort.debugMerge = false;
        ExternalMergeSort.debug = false;
        ExternalMergeSort<Data> sort = ExternalMergeSort.newSorter(new ExmesoDataSerializer(), Comparator.comparing(Data::getId))
                .withChunkSize(100_000)
                .withMaxOpenFiles(2000)
                .withDistinct(false)
                .withCleanup(true)
                .build();


        try (CloseableIterator<Data> it = sort.mergeSort(getDataStream(input).iterator())) {
            List<Data> list = StreamSupport.stream(Spliterators.spliterator(it, Long.MAX_VALUE, Spliterator.SORTED | Spliterator.NONNULL | Spliterator.ORDERED), false)
                    .onClose(() -> {
                        try {
                            it.close();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    })
                    .skip(SKIP)
                    .limit(LIMIT)
                    .collect(Collectors.toList());

            return list;
            /*
            int i = 0;
            while (it.hasNext()) {
                Data data = it.next();
                if (i >= SKIP && i < SKIP + LIMIT) {
                    list.add(data);
                }

                if (i > SKIP + LIMIT) {
                    break;
                }

                i++;
            }
            */
        }
    }

    @Benchmark
    public List<Data> streamSort(Input input) {
        ExternalSortCollectors.Configuration<Data> configuration = ExternalSortCollectors.configuration(new DataSerializer())
                .withComparator(Comparator.comparing(Data::getId))
                .withInternalSortMaxItems(100_000)
                .withMaxRecordSize(1024)
                .withWriteBufferSize(16 * 4096)
                .build();

        long t1 = System.currentTimeMillis();

        try (Stream<Data> stream = getDataStream(input)
                .collect(ExternalSortCollectors.externalSort(configuration))) {
            List<Data> list = stream
                    .skip(SKIP)
                    .limit(LIMIT)
                    .collect(Collectors.toList());
            return list;
        }
    }

    private static Stream<Data> getDataStream(Input input) {
        Random random = new Random(SEED);

        return random.ints(STREAM_SIZE, 1, RAND_MAX +1)
                .mapToObj(id ->  dataWithPayload(id, input.payloadRepeat));
    }

    private static Data dataWithPayload(int id, int payloadRepeat) {
        StringBuilder sb = new StringBuilder(32);
        for (int i = 0; i < payloadRepeat; i++) {
            sb.append(id*17);
        }
        return new Data(id, sb.toString());

    }

    public static void main(String[] args) throws RunnerException {

        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");

        Options options = new OptionsBuilder()
                .include(SortStreamBenchmark.class.getName())
                .forks(1)
                .threads(1)
                .warmupIterations(4)
                .measurementIterations(4)
                .build();

        new Runner(options).run();



    }
}
