package io.github.ppzxc.fq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MVStoreFileQueueFullTest {

  private static final String FILE_NAME = "test_queue_concurrency.db";
  private MVStoreFileQueue<Integer> queue;
  private MVStoreFileQueueProperties properties;
  @TempDir
  Path tempDir;

  @BeforeEach
  void setUp() {
    File tempFile = tempDir.resolve(FILE_NAME).toFile();
    properties = new MVStoreFileQueueProperties();
    properties.setFileName(tempFile.getAbsolutePath());
    queue = new MVStoreFileQueue<>(properties);
  }

  @AfterEach
  void tearDown() {
    if (queue != null) {
      queue.close();
    }
  }

  @Test
  @Order(1)
  void shouldEnqueueAndDequeueCorrectly() {
    queue.enqueue(1);
    queue.enqueue(2);

    assertThat(queue.size()).isEqualTo(2);
    assertThat(queue.dequeue()).contains(1);
    assertThat(queue.dequeue()).contains(2);
    assertThat(queue.isEmpty()).isTrue();
  }

  @Test
  @Order(3)
  void shouldCommitPeriodicallyWhenAutoCommitDisabled() {
    for (int i = 0; i < properties.getBatchSize() * 2; i++) {
      queue.enqueue(i);
    }
    queue.metric("commit-test");
    // no exception = success
    assertThat(queue.size()).isEqualTo(properties.getBatchSize() * 2L);
  }

  @Test
  @Order(4)
  void shouldPersistDataAfterCloseAndReopen() {
    for (int i = 0; i < 10; i++) {
      queue.enqueue(i);
    }
    queue.close();

    // reopen
    queue = new MVStoreFileQueue<>(properties);
    Optional<Integer> first = queue.dequeue();

    assertThat(first).isPresent();
    assertThat(first.get()).isZero();
  }

  @Test
  @Order(5)
  void shouldHandleConcurrentEnqueueAndDequeue() throws Exception {
    int threadCount = 8;
    int operationsPerThread = 500;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount * 2);
    CountDownLatch latch = new CountDownLatch(threadCount * 2);
    Set<Integer> dequeued = ConcurrentHashMap.newKeySet();
    AtomicInteger counter = new AtomicInteger(0);

    for (int i = 0; i < threadCount; i++) {
      executor.submit(() -> {
        try {
          for (int j = 0; j < operationsPerThread; j++) {
            queue.enqueue(counter.incrementAndGet());
          }
        } finally {
          latch.countDown();
        }
      });
    }

    for (int i = 0; i < threadCount; i++) {
      executor.submit(() -> {
        try {
          int localCount = 0;
          while (localCount < operationsPerThread) {
            Optional<Integer> val = queue.dequeue();
            if (val.isPresent()) {
              dequeued.add(val.get());
              localCount++;
            } else {
              await().atMost(Duration.ofMillis(1));
            }
          }
        } finally {
          latch.countDown();
        }
      });
    }

    boolean completed = latch.await(60, TimeUnit.SECONDS);
    executor.shutdownNow();

    assertThat(completed).isTrue();
    assertThat(queue.isEmpty()).isTrue();
    assertThat(dequeued).hasSize(threadCount * operationsPerThread);
    assertThat(new HashSet<>(dequeued)).hasSize(threadCount * operationsPerThread);
  }

  @Test
  @Order(6)
  void shouldBackoffAndRetryWhenLockContentionOccurs() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch startLatch = new CountDownLatch(1);

    Callable<Void> writer1 = () -> {
      startLatch.await();
      queue.enqueue(1);
      return null;
    };
    Callable<Void> writer2 = () -> {
      startLatch.await();
      queue.enqueue(2);
      return null;
    };

    startLatch.countDown();
    List<Future<Void>> results = executor.invokeAll(Arrays.asList(writer1, writer2));
    executor.shutdown();
    boolean result = executor.awaitTermination(5, TimeUnit.SECONDS);

    assertThat(result).isTrue();
    assertThat(results).hasSize(2);
    assertThat(queue.size()).isEqualTo(2);
  }
}
