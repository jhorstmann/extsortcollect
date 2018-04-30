package net.jhorstmann.extsortcollect;

import org.geirove.exmeso.CloseableIterator;
import org.geirove.exmeso.ExternalMergeSort;

import java.io.*;
import java.util.*;

public class ExmesoBenchmark {
    static class ExmesoDataSerializer implements ExternalMergeSort.Serializer<Data> {

        private static final int BUFFER_SIZE = 64 * 4096;

        @Override
        public void writeValues(Iterator<Data> iterator, OutputStream out) throws IOException {
            try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(out, BUFFER_SIZE))) {
                while (iterator.hasNext()) {
                    Data data = iterator.next();
                    dos.writeInt(data.getId());
                    String key = data.getKey();
                    dos.writeInt(key.length());
                    dos.writeChars(key);
                    String payload = data.getPayload();
                    dos.writeInt(payload.length());
                    dos.writeChars(payload);
                }

            }
        }

        @Override
        public Iterator<Data> readValues(InputStream in) throws IOException {
            return new DataIterator(new DataInputStream(new BufferedInputStream(in, BUFFER_SIZE)));
        }

        private static class DataIterator implements Iterator<Data> {
            private final DataInputStream dis;
            private Data current;

            DataIterator(DataInputStream dis) {
                this.dis = dis;
            }

            private Data readNext() {
                try {
                    int id = dis.readInt();
                    int keylen = dis.readInt();
                    char[] key = new char[keylen];
                    for (int i = 0; i < keylen; i++) {
                        key[i] = dis.readChar();
                    }
                    int payloadlen = dis.readInt();
                    char[] payload = new char[payloadlen];
                    for (int i = 0; i < payloadlen; i++) {
                        payload[i] = dis.readChar();
                    }
                    return new Data(id, new String(key), new String(payload));
                } catch (EOFException e) {
                    return null;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }


            @Override
            public boolean hasNext() {
                if (current == null) {
                    current = readNext();
                }
                return current != null;
            }

            @Override
            public Data next() {
                if (current == null) {
                    current = readNext();
                }

                if (current == null) {
                    throw new NoSuchElementException();
                } else {
                    Data data = current;
                    current = null;
                    return data;
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {

        ExmesoDataSerializer serializer = new ExmesoDataSerializer();
        Comparator<Data> comparator = Comparator.comparing(Data::getId);

        ExternalMergeSort.debugMerge = true;
        ExternalMergeSort<Data> sort = ExternalMergeSort.newSorter(serializer, comparator)
                .withChunkSize(100_000)
                .withMaxOpenFiles(2000)
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
