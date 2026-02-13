package io.github.ppzxc.fq;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.extern.slf4j.Slf4j;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.MVStore.Builder;

@Slf4j
class MVStoreFileQueue<T extends Serializable> implements FileQueue<T> {

  private static final String[] UNITS = new String[]{"B", "kB", "MB", "GB", "TB", "PB", "EB"};
  private final String fileName;
  private final MVStoreFileQueueProperties properties;
  private final ReentrantReadWriteLock lock;
  private final MVStore mvStore;
  private final MVMap<Long, T> queue;
  private final AtomicLong head;
  private final AtomicLong tail;
  private final AtomicLong totalOperation;
  private final AtomicLong totalCommits;
  private volatile boolean closed = false;

  MVStoreFileQueue(MVStoreFileQueueProperties properties, String fileName) {
    this.fileName = fileName;
    this.properties = properties;
    this.lock = new ReentrantReadWriteLock(properties.isFair());
    if (properties.getQueueName() == null || properties.getQueueName().trim().isEmpty()) {
      throw new IllegalArgumentException("[MVStoreFileQueue] MVStoreFileQueueProperties.queueName cannot be null or empty");
    }

    try {
      Path path = Paths.get(fileName);
      if (Files.notExists(path)) {
        Files.createDirectories(path.getParent());
      }
      Builder builder = new Builder()
        .fileName(fileName)
        .autoCompactFillRate(properties.getAutoCompactFillRate())
        .cacheSize(properties.getCacheSize()) // read
        .autoCommitBufferSize(properties.getAutoCommitBufferSize()) // write
        .backgroundExceptionHandler((t, e) -> log.error("message=uncaught exception", e));
      if (properties.isUseCompress()) {
        builder.compress();
      }
      if (properties.isAutoCommitDisabled()) {
        builder.autoCommitDisabled();
      }
      this.mvStore = builder.open();
      this.queue = mvStore.openMap(properties.getQueueName());
      this.head = new AtomicLong();
      this.tail = new AtomicLong();
      this.totalOperation = new AtomicLong();
      this.totalCommits = new AtomicLong();

      acquireWriteLock(() -> {
        this.head.set(queue.firstKey() != null ? queue.firstKey() : 0);
        this.tail.set(queue.lastKey() != null ? queue.lastKey() + 1 : 0);
        this.totalOperation.set(0);
        this.totalCommits.set(0);
        return null;
      });
      log.info("head={} tail={} message=mvStore file queue initialized", head.get(), tail.get());
    } catch (Exception e) {
      throw new FileQueueException("[MVStoreFileQueue] Failed to initialize queue", e);
    }
  }

  @Override
  public String fileName() {
    return fileName;
  }

  @Override
  public boolean enqueue(T value) {
    checkClosed();
    if (value == null) {
      throw new IllegalArgumentException("[MVStoreFileQueue] value cannot be null");
    }
    return acquireWriteLock(() -> {
      if (size() >= properties.getMaxSize()) {
        throw new FileQueueException("[MVStoreFileQueue] Queue is full: " + size() + " > " + properties.getMaxSize());
      }
      try {
        long key = tail.getAndIncrement();
        if (queue.put(key, value) != null) {
          log.warn("key={} value={} message=duplicate key", key, value);
        }
        return true;
      } finally {
        commitIfNeeded();
      }
    });
  }

  @Override
  public boolean enqueue(List<T> value) {
    checkClosed();
    if (value == null) {
      throw new IllegalArgumentException("[MVStoreFileQueue] value list cannot be null");
    }
    if (value.isEmpty()) {
      return true;
    }
    // Validate all elements before acquiring lock
    for (T v : value) {
      if (v == null) {
        throw new IllegalArgumentException("[MVStoreFileQueue] value in list cannot be null");
      }
    }
    return acquireWriteLock(() -> {
      if (size() + value.size() >= properties.getMaxSize()) {
        throw new FileQueueException("Queue is full: " + size() + " > " + properties.getMaxSize());
      }
      value.forEach(v -> queue.put(tail.getAndIncrement(), v));
      commitIfNeeded();
      return true;
    });
  }

  @Override
  public T dequeue() {
    checkClosed();
    return acquireWriteLock(() -> {
      if (isEmpty()) {
        return null;
      }
      try {
        return queue.remove(head.getAndIncrement());
      } finally {
        commitIfNeeded();
      }
    });
  }

  @Override
  public List<T> dequeue(int size) {
    checkClosed();
    if (size < 0) {
      throw new IllegalArgumentException("[MVStoreFileQueue] size cannot be negative");
    }
    if (size == 0) {
      return new ArrayList<>();
    }
    return acquireWriteLock(() -> {
      List<T> dequeued = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        if (isEmpty()) {
          break;
        }
        dequeued.add(queue.remove(head.getAndIncrement()));
      }
      commitIfNeeded();
      return dequeued;
    });
  }

  @Override
  public boolean isEmpty() {
    checkClosed();
    return acquireReadLock(() -> head.get() == tail.get());
  }

  @Override
  public long size() {
    checkClosed();
    return acquireReadLock(() -> tail.get() - head.get());
  }

  @Override
  public void metric(String name) {
    log.info("name={} total.operations={} total.commits={} current.size={} head={} tail={}", name,
      totalOperation.get(), totalCommits.get(), tail.get() - head.get(), head.get(), tail.get());
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    acquireWriteLock(() -> {
      if (closed) {
        return null;
      }
      closed = true;
      long newVersion = mvStore.commit();
      log.info("newVersion={} message=close", newVersion);
      mvStore.close();
      return null;
    });
  }

  private void checkClosed() {
    if (closed) {
      throw new IllegalStateException("[MVStoreFileQueue] Queue is already closed");
    }
  }

  @Override
  public void compactFile() {
    checkClosed();
    acquireWriteLock(() -> {
      long before = fileSize();
      if (before > properties.getCompactByFileSize()) {
        long started = System.nanoTime();
        mvStore.compactFile(properties.getMaxCompactTime());
        log.info("before.file.size={} after.file.size={} latency={} file.name={} message=compact mvStore file queue",
          readableFileSize(before), readableFileSize(fileSize()), latency(started), fileName);
      } else {
        log.info("file.size={} compact.by={} file.name={} message=compact mvStore file queue", readableFileSize(before),
          readableFileSize(properties.getCompactByFileSize()), fileName);
      }
      return null;
    });
  }

  private String latency(long started) {
    long nanos = System.nanoTime() - started;
    TimeUnit unit = chooseUnit(nanos);
    double value = (double)nanos / (double)TimeUnit.NANOSECONDS.convert(1L, unit);
    String var10000 = String.format(Locale.ROOT, "%.4g", value);
    return var10000 + " " + abbreviate(unit);
  }

  private <R> R acquireWriteLock(SupplierWithException<R, Exception> action) {
    Exception exception = null;
    for (int attempt = 0; attempt < properties.getMaxRetry(); attempt++) {
      lock.writeLock().lock();
      try {
        return action.get();
      } catch (Exception e) {
        log.info("[MVStoreFileQueue] Failed to acquire write lock: retry {}", attempt, e);
        exception = e;
        sleepBackoff();
      } finally {
        lock.writeLock().unlock();
      }
    }
    throw new FileQueueException(
      "[MVStoreFileQueue] Failed to acquire write lock after " + properties.getMaxRetry() + " attempts", exception);
  }

  private <R> R acquireReadLock(SupplierWithException<R, Exception> action) {
    Exception exception = null;
    for (int attempt = 0; attempt < properties.getMaxRetry(); attempt++) {
      lock.readLock().lock();
      try {
        return action.get();
      } catch (Exception e) {
        log.info("Failed to acquire read lock: retry {}", attempt, e);
        exception = e;
        sleepBackoff();
      } finally {
        lock.readLock().unlock();
      }
    }
    throw new FileQueueException(
      "[MVStoreFileQueue] Failed to acquire read lock after " + properties.getMaxRetry() + " attempts", exception);
  }

  private void commitIfNeeded() {
    if (totalOperation.incrementAndGet() % properties.getBatchSize() == 0) {
      if (properties.isAutoCommitDisabled()) {
        if (log.isDebugEnabled()) {
          log.debug("total={} commits={} batch={} message=commit", totalOperation.get(),
            totalCommits.get(), properties.getBatchSize());
        }
        mvStore.commit();
        totalCommits.incrementAndGet();
      } else {
        if (log.isDebugEnabled()) {
          log.debug("total={} commits={} batch={} message=enabled auto commit",
            totalOperation.get(),
            totalCommits.get(), properties.getBatchSize());
        }
      }
    }
  }

  @FunctionalInterface
  private interface SupplierWithException<R, E extends Exception> {

    R get() throws E;
  }

  private void sleepBackoff() {
    try {
      Thread.sleep(properties.getRetryBackoffMs());
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      log.warn("Backoff sleep interrupted", ie);
    }
  }

  private long fileSize() {
    try {
      return Files.size(Paths.get(fileName));
    } catch (IOException e) {
      log.error("message=get file size error", e);
    }
    return 0;
  }

  private String readableFileSize(long size) {
    if (size <= 0) {
      return "0";
    }
    int digitGroups = (int) (Math.log10((double) size) / Math.log10(1024));
    return new DecimalFormat("#,##0.#").format(size / Math.pow(1024, digitGroups)) + " " + UNITS[digitGroups];
  }

  private TimeUnit chooseUnit(long nanos) {
    if (TimeUnit.DAYS.convert(nanos, TimeUnit.NANOSECONDS) > 0L) {
      return TimeUnit.DAYS;
    } else if (TimeUnit.HOURS.convert(nanos, TimeUnit.NANOSECONDS) > 0L) {
      return TimeUnit.HOURS;
    } else if (TimeUnit.MINUTES.convert(nanos, TimeUnit.NANOSECONDS) > 0L) {
      return TimeUnit.MINUTES;
    } else if (TimeUnit.SECONDS.convert(nanos, TimeUnit.NANOSECONDS) > 0L) {
      return TimeUnit.SECONDS;
    } else if (TimeUnit.MILLISECONDS.convert(nanos, TimeUnit.NANOSECONDS) > 0L) {
      return TimeUnit.MILLISECONDS;
    } else {
      return TimeUnit.MICROSECONDS.convert(nanos, TimeUnit.NANOSECONDS) > 0L ? TimeUnit.MICROSECONDS : TimeUnit.NANOSECONDS;
    }
  }

  private String abbreviate(TimeUnit unit) {
    switch (unit) {
      case NANOSECONDS:
        return "ns";
      case MICROSECONDS:
        return "μs";
      case MILLISECONDS:
        return "ms";
      case SECONDS:
        return "s";
      case MINUTES:
        return "min";
      case HOURS:
        return "h";
      case DAYS:
        return "d";
      default:
        throw new AssertionError();
    }
  }
}
