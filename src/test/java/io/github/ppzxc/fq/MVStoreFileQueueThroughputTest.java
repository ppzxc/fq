package io.github.ppzxc.fq;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class MVStoreFileQueueThroughputTest {

  private static final String FILE_NAME = "test_queue.db";
  private FileQueue<String> fileQueue;
  @TempDir
  Path tempDir;

  @AfterEach
  void tearDown() {
    if (fileQueue != null) {
      fileQueue.close();
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
    List<String> results = new ArrayList<>();
    MVStoreFileQueueProperties mvStoreFileQueueProperties = new MVStoreFileQueueProperties();
    mvStoreFileQueueProperties.setBatchSize(batchSize);
    fileQueue = FileQueueFactory.createMVStoreFileQueue(tempDir.resolve(FILE_NAME).toFile().getAbsolutePath(),
      mvStoreFileQueueProperties);

    Stopwatch enqueueStartTime = Stopwatch.createStarted();
    // enqueue
    for (int i = 0; i < operations; i++) {
      fileQueue.enqueue("ITEM " + i);
    }
    Stopwatch enqueueEndTime = enqueueStartTime.stop();
    Stopwatch dequeueStartTime = Stopwatch.createStarted();
    // dequeue
    for (int i = 0; i < operations; i++) {
      String dequeue = fileQueue.dequeue();
      if (dequeue != null) {
        results.add(dequeue);
      }
    }
    Stopwatch dequeueEndTime = dequeueStartTime.stop();

    System.out.printf("BatchSize=%d operations=%d%n", batchSize, operations);
    System.out.printf("enqueue=%s%n", enqueueEndTime);
    System.out.printf("dequeue=%s%n", dequeueEndTime);
    System.out.println("----------------------------");
    fileQueue.metric("TEST");
    assertThat(results).hasSize(operations);
  }

  @Timeout(1000 * 60 * 5)
  @DisplayName("all enqueue, all dequeue on multi thread")
  @ParameterizedTest
  @CsvSource({
    "2, 10000, 1000000",
  })
  void t2(int threads, int batchSize, int operations) throws InterruptedException {
    ReentrantLock lock = new ReentrantLock();
    List<String> results = new ArrayList<>();
    MVStoreFileQueueProperties mvStoreFileQueueProperties = new MVStoreFileQueueProperties();
    mvStoreFileQueueProperties.setBatchSize(batchSize);
    fileQueue = FileQueueFactory.createMVStoreFileQueue(tempDir.resolve(FILE_NAME).toFile().getAbsolutePath(),
      mvStoreFileQueueProperties);

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
            String item = fileQueue.dequeue();
            if (item != null) {
              lock.lock();
              try {
                results.add(item);
              } finally {
                lock.unlock();
              }
            }
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
    assertThat(results).hasSize(operations * threads);
  }

  //  @Timeout(1000 * 60 * 5)
  @DisplayName("large size payload")
  @ParameterizedTest
  @CsvSource({
    "1024, 1000, 100000",
    "2048, 10000, 100000",
    "4096, 100000, 100000",
    "8192, 1000000, 100000",
    "1024, 1000, 100000",
    "2048, 1000, 100000",
    "4096, 1000, 100000",
    "8192, 1000, 100000"
  })
  void t3(int payloadLength, int batchSize, int operations) {
    MVStoreFileQueueProperties mvStoreFileQueueProperties = new MVStoreFileQueueProperties();
    mvStoreFileQueueProperties.setBatchSize(batchSize);
    fileQueue = FileQueueFactory.createMVStoreFileQueue(tempDir.resolve(FILE_NAME).toFile().getAbsolutePath(),
      mvStoreFileQueueProperties);

    Stopwatch enqueueStartTime = Stopwatch.createStarted();
    // enqueue
    for (int i = 0; i < operations; i++) {
      fileQueue.enqueue(getAlphabet(payloadLength) + i);
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
    fileQueue.close();
    assertThat(fileQueue.isEmpty()).isTrue();
  }

  private String getAlphabet(int targetStringLength) {
    int leftLimit = 97; // letter 'a'
    int rightLimit = 122; // letter 'z'
    StringBuilder buffer = new StringBuilder(targetStringLength);
    for (int i = 0; i < targetStringLength; i++) {
      int randomLimitedInt = leftLimit + (int)
        (ThreadLocalRandom.current().nextFloat() * (rightLimit - leftLimit + 1));
      buffer.append((char) randomLimitedInt);
    }
    return buffer.toString();
  }
}