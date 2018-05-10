# External merge sort for java8 streams

Sorting large streams of data without having to keep all elements in memory.

## Example usage

```java
ExternalSortCollectors.Serializer<T> serializer = ...
Comparator<T> comparator = ...

ExternalSortCollectors.Configuration<T> configuration = ExternalSortCollectors.configuration(serializer)
        .withComparator(comparator)
        .withInternalSortMaxItems(100_000)
        .withMaxRecordSize(1024)
        .withWriteBufferSize(64 * 4096)
        .build();

Stream<T> stream = ...

stream.collect(ExternalSortCollectors.externalSort(configuration))
        .skip(200_000)
        .limit(100)
        .foreach(record -> {
            ...
        });
```

## Comparison with [exmeso](https://github.com/grove/exmeso)

 - Based on NIO buffers instead of InputStream/OutputStream, this imposes a maximum record size which can be configured
 - Support for parallel sorting
 - About 1.5 times as fast according to benchmarks (without parallel sorting)
 - Sort is stable (See [issue #3 in exmeso](https://github.com/grove/exmeso/issues/3)
 - Temporary sorted chunks are stored in one large file instead of one file per chunk
 - Currently limited to a total file size of 2GB