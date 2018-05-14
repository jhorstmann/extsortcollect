package net.jhorstmann.extsortcollect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

class Cleaner implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(Cleaner.class);

    private interface Unmapper {
        void unmap(ByteBuffer buffer) throws Exception;
    }

    private static final Unmapper UNMAP;

    static {

        Unmapper unmap = null;
        try {
            // >=JDK9 class sun.misc.Unsafe { void invokeCleaner(ByteBuffer buf) }
            final Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            final Field f = unsafeClass.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            final Object theUnsafe = f.get(null);
            final Method method = unsafeClass.getDeclaredMethod("invokeCleaner", ByteBuffer.class);

            unmap = buffer -> method.invoke(theUnsafe, buffer);
        } catch (Exception e) {
            LOG.info("Could not access 'Unsafe.invokeCleaner' method");
        }

        if (unmap == null) {
            try {
                // <=JDK8 class DirectByteBuffer { sun.misc.Cleaner cleaner(Buffer buf) }
                //        then call sun.misc.Cleaner.clean
                final Class<?> directByteBufferClass = Class.forName("java.nio.DirectByteBuffer");
                Method getCleaner = directByteBufferClass.getMethod("cleaner");
                getCleaner.setAccessible(true);
                final Class<?> cleanerClass = Class.forName("sun.misc.Cleaner");
                Method clean = cleanerClass.getMethod("clean");
                clean.setAccessible(true);
                unmap = buffer -> {
                    Object cleaner = getCleaner.invoke(buffer);

                    if (cleaner != null) {
                        clean.invoke(cleaner);
                    }
                };
            } catch (Exception e) {
                LOG.info("Could not access 'DirectByteBuffer.cleaner' method");
            }
        }

        UNMAP = unmap;
    }

    static void clean(ByteBuffer buffer) {
        if (UNMAP != null) {
            try {
                UNMAP.unmap(buffer);
            } catch (Exception e) {
                LOG.info("Could not unmap buffer", e);
            }
        }
    }

    private final AtomicInteger referenceCount;
    private ByteBuffer buffer;

    Cleaner(AtomicInteger referenceCount, ByteBuffer buffer) {
        this.referenceCount = referenceCount;
        this.buffer = buffer;
        referenceCount.incrementAndGet();
    }

    @Override
    public void close() {
        if (referenceCount.decrementAndGet() == 0) {
            clean(buffer);
        }
        buffer = null;
    }

}
