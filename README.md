# External merge sort for java8 streams

Sorting large streams of data without having to keep all elements in memory.

## Example usage

```java
ExternalSortCollectors.Serializer<T> serializer = ...
Comparator<T> comparator = ...

ExternalSortCollectors.Configuration<Data> configuration = ExternalSortCollectors.configuration(serializer)
        .withComparator(comparator)
        .withInternalSortMaxItems(100_000)
        .withMaxRecordSize(1024)
        .withWriteBufferSize(64 * 4096)
        .withMaxNumberOfChunks(1024)
        .build();

stream.collect(ExternalSortCollectors.externalSort(configuration))
        .skip(200_000)
        .limit(100)
        .foreach(data -> {
            ...
        });
```

## Comparison with [exmeso](https://github.com/grove/exmeso)

 - Based on NIO buffers instead of InputStream/OutputStream
 - About twice as fast according to benchmarks
 - Sort is stable (See [issue #3 in exmeso](https://github.com/grove/exmeso/issues/3)
 - Temporary sorted chunks are stored in one large file instead of one file per chunk
 - Maximum record size is restricted to a configurable limit