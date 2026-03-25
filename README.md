# fq — File Queue

[![Maven Central](https://img.shields.io/maven-central/v/io.github.ppzxc/fq.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.ppzxc/fq)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build](https://github.com/ppzxc/fq/actions/workflows/main.yml/badge.svg)](https://github.com/ppzxc/fq/actions/workflows/main.yml)

[한국어](README.ko.md)

**fq** is a persistent, file-based FIFO queue for Java backed by [H2 MVStore](https://h2database.com/html/mvstore.html). It survives JVM restarts, is thread-safe, and is optimized for high-throughput serializable object storage.

## Features

- **Persistent** — survives JVM restarts; head/tail pointers are stored durably
- **Thread-safe** — fair `ReentrantReadWriteLock` with configurable retry + exponential backoff
- **High throughput** — configurable batch commits, optional compression
- **Bounded or unbounded** — optional `maxSize` cap
- **Auto compaction** — reclaims disk space when file exceeds a configurable threshold
- **Zero runtime dependencies** — only SLF4J API and H2 MVStore

## Installation

### Gradle

```groovy
dependencies {
    implementation 'io.github.ppzxc:fq:0.0.13'
}
```

### Maven

```xml
<dependency>
    <groupId>io.github.ppzxc</groupId>
    <artifactId>fq</artifactId>
    <version>0.0.13</version>
</dependency>
```

## Quick Start

```java
import io.github.ppzxc.fq.FileQueue;
import io.github.ppzxc.fq.FileQueueFactory;

// Create a queue with default settings (file: {user.dir}/sys/que/local)
FileQueue<String> queue = FileQueueFactory.createMVStoreFileQueue();

// Enqueue
queue.enqueue("hello");
queue.enqueue("world");

// Dequeue
String first = queue.dequeue();  // "hello"

// Batch dequeue
List<String> batch = queue.dequeue(10);

// Check state
long size = queue.size();
boolean empty = queue.isEmpty();

// Always close when done
queue.close();
```

### Custom Path & Properties

```java
import io.github.ppzxc.fq.MVStoreFileQueueProperties;

MVStoreFileQueueProperties props = MVStoreFileQueueProperties.builder()
    .batchSize(500)
    .maxSize(100_000L)
    .useCompress(true)
    .maxRetry(5)
    .build();

// Named queue under default directory
FileQueue<MyObject> queue = FileQueueFactory.createMVStoreFileQueue(props, "my-queue");

// Explicit path
FileQueue<MyObject> queue2 = FileQueueFactory.createMVStoreFileQueue("/data/queues/my-queue", props);
```

## Configuration

All options are set via `MVStoreFileQueueProperties`. Build with the provided builder:

| Property | Default | Description |
|---|---|---|
| `queueName` | `"queue"` | MVStore map name inside the file |
| `batchSize` | `1000` | Commit to disk every N writes |
| `maxSize` | `Long.MAX_VALUE` | Maximum number of elements (0 = unlimited) |
| `useCompress` | `true` | Enable LZF compression |
| `autoCommitDisabled` | `false` | Disable MVStore auto-commit |
| `autoCommitBufferSize` | `1024` | Auto-commit buffer size (KB) |
| `cacheSize` | `1` | Read cache size (MB) |
| `autoCompactFillRate` | `90` | MVStore fill-rate target for compaction (%) |
| `compactByFileSize` | `50` | Trigger `compactFile()` when file exceeds this (MB) |
| `maxCompactTime` | `60000` | Max time spent compacting per call (ms) |
| `fair` | `true` | Fair mode for `ReentrantReadWriteLock` |
| `maxRetry` | `3` | Max lock retry attempts (minimum 1) |
| `retryBackoffMs` | `100` | Exponential backoff base delay (ms); doubles per attempt, capped at 5 s |
| `allowedClasses` | `[]` | Whitelist of fully-qualified class names for safe deserialization (CWE-502); empty = allow all |

## API Reference

### `FileQueue<T extends Serializable>`

| Method | Description |
|---|---|
| `boolean enqueue(T value)` | Append a single element |
| `boolean enqueue(List<T> value)` | Append a list of elements atomically |
| `T dequeue()` | Remove and return the head element; `null` if empty |
| `List<T> dequeue(int size)` | Remove and return up to `size` elements |
| `boolean isEmpty()` | Return `true` if the queue has no elements |
| `long size()` | Return the current number of elements |
| `String fileName()` | Return the absolute path of the backing file |
| `void metric(String name)` | Log internal metrics (head, tail, size, counts) |
| `void compactFile()` | Compact the backing file if it exceeds the size threshold |
| `void close()` | Flush pending writes and close the store (idempotent) |

### `FileQueueFactory`

| Method | Description |
|---|---|
| `createMVStoreFileQueue()` | Default properties, file at `{user.dir}/sys/que/local` |
| `createMVStoreFileQueue(MVStoreFileQueueProperties, String fileName)` | Custom properties, file at `{user.dir}/sys/que/{fileName}` |
| `createMVStoreFileQueue(String path, MVStoreFileQueueProperties)` | Custom properties, explicit file path |

## Building

```bash
# Build and run all tests
./gradlew build

# Run tests only
./gradlew test

# Run a specific test class
./gradlew test --tests MVStoreFileQueueUnitTest

# Generate JaCoCo coverage report (build/reports/jacoco/test/html/)
./gradlew jacocoTestReport

# Run JMH benchmarks (build/results/jmh/results.json)
./gradlew jmh
```

## License

Licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).
