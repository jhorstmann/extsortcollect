package net.jhorstmann.extsortcollect.benchmark;

import org.geirove.exmeso.CloseableIterator;
import org.geirove.exmeso.ExternalMergeSort;

import java.io.*;
import java.util.*;

public class ExmesoBenchmark {

    public static void main(String[] args) throws IOException {

        ExmesoDataSerializer serializer = new ExmesoDataSerializer();
        Comparator<Data> comparator = Comparator.comparing(Data::getId);

        ExternalMergeSort.debugMerge = true;
        ExternalMergeSort<Data> sort = ExternalMergeSort.newSorter(serializer, comparator)
                .withChunkSize(100_000)
                .withMaxOpenFiles(2000)
                .withDistinct(false)
                .withCleanup(true)
                .build();

        long t1 = System.currentTimeMillis();

        int skip = 10_000;
        int limit = 100;

        List<Data> part = new ArrayList<>(1000);
        try (InputStream in = new BufferedInputStream(new FileInputStream("data/random.data"))) {
            try (CloseableIterator<Data> it = sort.mergeSort(serializer.readValues(in))) {
                int i = 0;
                while (it.hasNext()) {
                    Data data = it.next();
                    if (i >= skip && i < skip+ limit) {
                        part.add(data);
                    }

                    if (i > skip + limit) {
                        break;
                    }

                    i++;
                }
            }
        }
        System.out.println(part);

        System.out.println((System.currentTimeMillis() - t1) / 1000.0);
    }
}
