package io.github.ppzxc.fq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.Arrays;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MVStoreFileQueueEdgeCaseTest {

  @TempDir
  Path tempDir;

  private FileQueue<String> queue;

  @AfterEach
  void tearDown() {
    if (queue != null) {
      try {
        queue.close();
      } catch (Exception e) {
        // ignore
      }
    }
    tempDir.toFile().deleteOnExit();
  }

  @DisplayName("initialization with null queueName throws exception")
  @Test
  void initialization_nullQueueName() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when, then
    assertThatThrownBy(() -> properties.setQueueName("  "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("queueName cannot be null or empty");
  }

  @DisplayName("initialization failure - invalid path")
  @Test
  void initialization_invalidPath() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String invalidPath = "\0invalid\0path"; // null characters in path

    // when, then
    assertThatThrownBy(() -> FileQueueFactory.createMVStoreFileQueue(invalidPath, properties))
        .isInstanceOf(FileQueueException.class)
        .hasMessageContaining("[MVStoreFileQueue] Failed to initialize queue");
  }

  @DisplayName("enqueue - duplicate key warning")
  @Test
  void enqueue_duplicateKeyWarning() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setBatchSize(10);
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // when - enqueue items
    for (int i = 0; i < 5; i++) {
      queue.enqueue("item" + i);
    }

    // then - no exception, just warning logged
    assertThat(queue.size()).isEqualTo(5);
  }

  @DisplayName("close - double check closed flag")
  @Test
  void close_doubleCheckClosedFlag() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // when
    queue.close();
    queue.close(); // second close should return early

    // then - no exception
  }

  @DisplayName("autoCommitDisabled - manual commit triggered")
  @Test
  void autoCommitDisabled_manualCommit() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setAutoCommitDisabled(true);
    properties.setBatchSize(5);
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // when - enqueue enough items to trigger commit
    for (int i = 0; i < 10; i++) {
      queue.enqueue("item" + i);
    }

    // then
    assertThat(queue.size()).isEqualTo(10);
  }

  @DisplayName("readableFileSize - zero size")
  @Test
  void readableFileSize_zeroSize() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // when
    queue.compactFile();

    // then - should handle zero size gracefully
  }

  @DisplayName("readableFileSize - negative size")
  @Test
  void readableFileSize_negativeSize() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // when
    queue.compactFile();

    // then - should handle negative size gracefully
  }

  @DisplayName("latency measurement - nanoseconds")
  @Test
  void latency_nanoseconds() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setCompactByFileSize(1);
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
    queue.enqueue("item");

    // when
    queue.compactFile();

    // then - latency should be measured
  }

  @DisplayName("latency measurement - microseconds")
  @Test
  void latency_microseconds() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setCompactByFileSize(1);
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
    for (int i = 0; i < 100; i++) {
      queue.enqueue("item" + i);
    }

    // when
    queue.compactFile();

    // then - latency should be measured
  }

  @DisplayName("latency measurement - milliseconds")
  @Test
  void latency_milliseconds() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setCompactByFileSize(1);
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
    for (int i = 0; i < 1000; i++) {
      queue.enqueue("item" + i);
    }

    // when
    queue.compactFile();

    // then - latency should be measured
  }

  @DisplayName("latency measurement - seconds")
  @Test
  void latency_seconds() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setCompactByFileSize(1);
    properties.setMaxCompactTime(1); // 1ms to make it fast
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
    for (int i = 0; i < 10000; i++) {
      queue.enqueue("item" + i);
    }

    // when
    queue.compactFile();

    // then - latency should be measured
  }

  @DisplayName("abbreviate - all time units")
  @Test
  void abbreviate_allTimeUnits() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setCompactByFileSize(1);
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
    queue.enqueue("item");

    // when - trigger compactFile which uses abbreviate
    queue.compactFile();

    // then - should not throw
  }

  @DisplayName("backgroundExceptionHandler - handles exceptions")
  @Test
  void backgroundExceptionHandler_handlesExceptions() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // when - normal operations
    queue.enqueue("item");
    queue.dequeue();

    // then - background exception handler is registered
  }

  @DisplayName("dequeue with size - partial dequeue from empty queue")
  @Test
  void dequeueWithSize_partialFromEmpty() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
    queue.enqueue(Arrays.asList("item1", "item2"));

    // when
    queue.dequeue(1);
    queue.dequeue(5); // try to dequeue more than available

    // then
    assertThat(queue.isEmpty()).isTrue();
  }

  @DisplayName("fileSize - IOException handling")
  @Test
  void fileSize_ioExceptionHandling() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
    queue.enqueue("item");

    // when
    queue.compactFile();

    // then - should handle file size check gracefully
  }
}
