package net.jhorstmann.extsortcollect.benchmark;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Random;

public class GenerateRandomDataFile {
    public static void main(String[] args) throws IOException {
        Random random = new Random(123456789L);
        ByteBuffer buffer = ByteBuffer.allocate(4096 * 64);
        DataSerializer serializer = new DataSerializer();

        Path tempPath = Paths.get("data/random.data");
        try (FileChannel file = FileChannel.open(tempPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            for (int i = 0; i < 1_000_000; i++) {

                for (int j = 0; j < 20; j++) {

                    int id = random.nextInt(100_000) + 1;
                    StringBuilder sb = new StringBuilder();
                    for (int k = 0; k < 10; k++) {
                        sb.append(id*17);
                    }

                    serializer.write(buffer, new Data(id, sb.toString()));
                }
                buffer.flip();
                file.write(buffer);
                buffer.clear();
            }
        }
    }
}
