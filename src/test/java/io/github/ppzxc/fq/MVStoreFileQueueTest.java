package io.github.ppzxc.fq;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.base.Stopwatch;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class MVStoreFileQueueTest {

  private FileQueue<String> fileQueue;
  private String fileName = "test_queue.db";

  @BeforeEach
  void setUp() {
    File file = new File(fileName);
    if (file.exists()) {
      file.delete();
    }
  }

  @AfterEach
  void tearDown() {
    if (fileQueue != null) {
      fileQueue.close();
    }
    File file = new File(fileName);
    if (file.exists()) {
      file.delete();
    }
  }

  @DisplayName("enqueue, dequeue test")
  @Test
  void t0() {
    // given
    MVStoreFileQueueProperties mvStoreFileQueueProperties = new MVStoreFileQueueProperties();
    mvStoreFileQueueProperties.setFileName(fileName);
    mvStoreFileQueueProperties.setBatchSize(100);
    fileQueue = FileQueueFactory.createMVStoreFileQueue(mvStoreFileQueueProperties);
    List<String> given = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      given.add(UUID.randomUUID().toString().replaceAll("-", ""));
    }

    // when
    given.forEach(fileQueue::enqueue);
    List<String> actual = new ArrayList<>();
    while (!fileQueue.isEmpty()) {
      actual.add(fileQueue.dequeue().get());
    }

    // then
    assertThat(actual.size()).isEqualTo(given.size());
    for (int i = 0; i < given.size(); i++) {
      assertThat(actual.get(i)).isEqualTo(given.get(i));
    }
  }

  @Timeout(1000 * 60 * 5)
  @DisplayName("all enqueue, all dequeue on single thread")
  @ParameterizedTest
  @CsvSource({
      "1000, 1000000",
      "10000, 1000000",
      "100000, 1000000",
      "1000000, 1000000"
  })
  void t1(int batchSize, int operations) {
    MVStoreFileQueueProperties mvStoreFileQueueProperties = new MVStoreFileQueueProperties();
    mvStoreFileQueueProperties.setFileName(fileName);
    mvStoreFileQueueProperties.setBatchSize(batchSize);
    fileQueue = FileQueueFactory.createMVStoreFileQueue(mvStoreFileQueueProperties);

    Stopwatch enqueueStartTime = Stopwatch.createStarted();
    // enqueue
    for (int i = 0; i < operations; i++) {
      fileQueue.enqueue("ITEM " + i);
    }
    Stopwatch enqueueEndTime = enqueueStartTime.stop();
    Stopwatch dequeueStartTime = Stopwatch.createStarted();
    // dequeue
    for (int i = 0; i < operations; i++) {
      fileQueue.dequeue();
    }
    Stopwatch dequeueEndTime = dequeueStartTime.stop();

    System.out.printf("BatchSize=%d operations=%d%n", batchSize, operations);
    System.out.printf("enqueue=%s%n", enqueueEndTime);
    System.out.printf("dequeue=%s%n", dequeueEndTime);
    System.out.println("----------------------------");
    fileQueue.metric("TEST");
  }

  @Timeout(1000 * 60 * 5)
  @DisplayName("all enqueue, all dequeue on multi thread")
  @ParameterizedTest
  @CsvSource({
      "2, 10000, 1000000",
  })
  void t2(int threads, int batchSize, int operations) throws InterruptedException {
    MVStoreFileQueueProperties mvStoreFileQueueProperties = new MVStoreFileQueueProperties();
    mvStoreFileQueueProperties.setFileName(fileName);
    mvStoreFileQueueProperties.setBatchSize(batchSize);
    fileQueue = FileQueueFactory.createMVStoreFileQueue(mvStoreFileQueueProperties);

    // enqueue
    Stopwatch enqueueStartTime = Stopwatch.createStarted();
    Thread[] enqueueThreads = new Thread[threads];
    for (int i = 0; i < threads; i++) {
      enqueueThreads[i] = new Thread(() -> {
        for (int j = 0; j < operations; j++) {
          try {
            fileQueue.enqueue("ITEM " + j);
          } catch (FileQueueException e) {
            throw new RuntimeException(e);
          }
        }
      });
      enqueueThreads[i].start();
    }
    for (Thread enqueueThread : enqueueThreads) {
      enqueueThread.join();
    }
    Stopwatch enqueueEndTime = enqueueStartTime.stop();

    // dequeue
    Stopwatch dequeueStartTime = Stopwatch.createStarted();
    Thread[] dequeueThreads = new Thread[threads];
    for (int i = 0; i < threads; i++) {
      dequeueThreads[i] = new Thread(() -> {
        for (int j = 0; j < operations; j++) {
          try {
            fileQueue.dequeue();
          } catch (FileQueueException e) {
            throw new RuntimeException(e);
          }
        }
      });
      dequeueThreads[i].start();
    }
    for (Thread dequeueThread : dequeueThreads) {
      dequeueThread.join();
    }
    Stopwatch dequeueEndTime = dequeueStartTime.stop();

    System.out.printf("BatchSize=%d operations=%d%n", batchSize, operations);
    System.out.printf("enqueue=%s%n", enqueueEndTime);
    System.out.printf("dequeue=%s%n", dequeueEndTime);
    System.out.println("----------------------------");
    fileQueue.metric("TEST");
  }
}