package net.jhorstmann.extsortcollect.benchmark;

import org.geirove.exmeso.ExternalMergeSort;

import java.io.*;
import java.util.Iterator;
import java.util.NoSuchElementException;

class ExmesoDataSerializer implements ExternalMergeSort.Serializer<Data> {

    private static final int BUFFER_SIZE = 64 * 4096;

    @Override
    public void writeValues(Iterator<Data> iterator, OutputStream out) throws IOException {
        try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(out, BUFFER_SIZE))) {
            while (iterator.hasNext()) {
                Data data = iterator.next();
                dos.writeInt(data.getId());
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
                if (id <= 0) {
                    throw new IllegalStateException("invalid id " + id);
                }

                int payloadlen = dis.readInt();
                if (payloadlen <= 0) {
                    throw new IllegalStateException("negative length " + payloadlen);
                }

                char[] payload = new char[payloadlen];
                for (int i = 0; i < payloadlen; i++) {
                    payload[i] = dis.readChar();
                    if (payload[i] < '0' || payload[i] > '9') {
                        throw new IllegalStateException("not a digit " + payload[i]);
                    }
                }
                return new Data(id, new String(payload));
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
