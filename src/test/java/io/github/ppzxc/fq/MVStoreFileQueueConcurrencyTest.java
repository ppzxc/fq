package io.github.ppzxc.fq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
class MVStoreFileQueueConcurrencyTest {

  private static final String FILE_NAME = "test_queue_concurrency.db";
  private MVStoreFileQueue<Integer> queue;
  @TempDir
  Path tempDir;

  @BeforeEach
  void setUp() {
    File tempFile = tempDir.resolve(FILE_NAME).toFile();
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    queue = new MVStoreFileQueue<>(properties, tempFile.getAbsolutePath());
  }

  @AfterEach
  void tearDown() {
    if (queue != null) {
      queue.close();
    }
  }

  @Test
  @Order(1)
  void shouldHandleConcurrentEnqueueAndDequeueWithoutDataLoss() throws Exception {
    int threadCount = 10;
    int operationsPerThread = 1000;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount * 2);
    CountDownLatch latch = new CountDownLatch(threadCount * 2);
    Set<Integer> dequeued = ConcurrentHashMap.newKeySet();
    AtomicInteger counter = new AtomicInteger(0);

    // Enqueue threads
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

    // Dequeue threads
    for (int i = 0; i < threadCount; i++) {
      executor.submit(() -> {
        try {
          int localCount = 0;
          while (localCount < operationsPerThread) {
            Integer value = queue.dequeue();
            if (value != null) {
              dequeued.add(value);
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

    assertThat(completed)
      .as("All threads should complete within timeout")
      .isTrue();

    assertThat(queue.isEmpty())
      .as("Queue should be empty after all operations")
      .isTrue();

    assertThat(dequeued)
      .as("No data loss or duplication expected")
      .hasSize(threadCount * operationsPerThread);

    assertThat(new HashSet<>(dequeued))
      .as("All dequeued elements should be unique")
      .hasSize(threadCount * operationsPerThread);
  }
}
