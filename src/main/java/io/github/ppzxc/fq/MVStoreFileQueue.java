package io.github.ppzxc.fq;

import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.extern.slf4j.Slf4j;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.MVStore.Builder;

@Slf4j
class MVStoreFileQueue<T extends Serializable> implements FileQueue<T> {

  private final MVStoreFileQueueProperties properties;
  private final ReentrantReadWriteLock lock;
  private final MVStore mvStore;
  private final MVMap<Long, T> queue;
  private final AtomicLong head;
  private final AtomicLong tail;
  private final AtomicLong totalOperation;
  private final AtomicLong totalCommits;

  MVStoreFileQueue(MVStoreFileQueueProperties properties) {
    this.properties = properties;
    this.lock = new ReentrantReadWriteLock(properties.isFair());

    try {
      Builder builder = new Builder()
          .fileName(properties.getFileName())
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
    } catch (Exception e) {
      throw new FileQueueException("Failed to initialize queue", e);
    }
  }

  @Override
  public void enqueue(T value) {
    acquireWriteLock(() -> {
      if (size() >= properties.getMaxSize()) {
        throw new FileQueueException("Queue is full: " + size() + " > " + properties.getMaxSize());
      }
      queue.put(tail.getAndIncrement(), value);
      commitIfNeeded();
      return null;
    });
  }

  @Override
  public Optional<T> dequeue() {
    return acquireWriteLock(() -> {
      if (isEmpty()) {
        return Optional.empty();
      }
      T item = queue.remove(head.getAndIncrement());
      commitIfNeeded();
      return Optional.ofNullable(item);
    });
  }

  @Override
  public boolean isEmpty() {
    return acquireReadLock(() -> head.get() == tail.get());
  }

  @Override
  public long size() {
    return acquireReadLock(() -> tail.get() - head.get());
  }

  @Override
  public void metric(String name) {
    log.info("name={} total.operations={} total.commits={} current.size={} head={} tail={}", name,
        totalOperation, totalCommits, tail.get() - head.get(), head, tail);
  }

  @Override
  public void close() {
    acquireWriteLock(() -> {
      mvStore.close();
      return null;
    });
  }

  private <R> R acquireWriteLock(SupplierWithException<R, Exception> action) {
    for (int attempt = 0; attempt < properties.getMaxRetry(); attempt++) {
      lock.writeLock().lock();
      try {
        return action.get();
      } catch (Exception e) {
        log.info("Failed to acquire write lock: retry {}", attempt, e);
      } finally {
        lock.writeLock().unlock();
      }
    }
    throw new FileQueueException(
        "Failed to acquire write lock after " + properties.getMaxRetry() + " attempts");
  }

  private <R> R acquireReadLock(SupplierWithException<R, Exception> action) {
    for (int attempt = 0; attempt < properties.getMaxRetry(); attempt++) {
      lock.readLock().lock();
      try {
        return action.get();
      } catch (Exception e) {
        log.info("Failed to acquire read lock: retry {}", attempt, e);
      } finally {
        lock.readLock().unlock();
      }
    }
    throw new FileQueueException(
        "Failed to acquire read lock after " + properties.getMaxRetry() + " attempts");
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
}
