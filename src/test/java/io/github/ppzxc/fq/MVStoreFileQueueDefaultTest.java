package io.github.ppzxc.fq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MVStoreFileQueueDefaultTest {

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

  @DisplayName("enqueue, dequeue test")
  @Test
  void t0() {
    // given
    MVStoreFileQueueProperties mvStoreFileQueueProperties = new MVStoreFileQueueProperties();
    mvStoreFileQueueProperties.setFileName(tempDir.resolve(FILE_NAME).toFile().getAbsolutePath());
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
      fileQueue.dequeue().ifPresent(actual::add);
    }

    // then
    assertThat(actual).hasSameSizeAs(given);
    for (int i = 0; i < given.size(); i++) {
      assertThat(actual.get(i)).isEqualTo(given.get(i));
    }
  }

  @DisplayName("fileName cannot be null or empty")
  @Test
  void t1() {
    // given
    MVStoreFileQueueProperties mvStoreFileQueueProperties = new MVStoreFileQueueProperties();
    mvStoreFileQueueProperties.setFileName(null);

    // when, then
    assertThatCode(() -> FileQueueFactory.createMVStoreFileQueue(mvStoreFileQueueProperties))
      .isInstanceOf(IllegalArgumentException.class)
      .isInstanceOfSatisfying(IllegalArgumentException.class, exception -> assertThat(exception.getMessage()).isEqualTo(
        "[MVStoreFileQueue] MVStoreFileQueueProperties.fileName cannot be null or empty"));
  }

  @DisplayName("queueName cannot be null or empty")
  @Test
  void t2() {
    // given
    MVStoreFileQueueProperties mvStoreFileQueueProperties = new MVStoreFileQueueProperties();
    mvStoreFileQueueProperties.setFileName("test_queue.db");
    mvStoreFileQueueProperties.setQueueName(null);

    // when, then
    assertThatCode(() -> FileQueueFactory.createMVStoreFileQueue(mvStoreFileQueueProperties))
      .isInstanceOf(IllegalArgumentException.class)
      .isInstanceOfSatisfying(IllegalArgumentException.class, exception -> assertThat(exception.getMessage()).isEqualTo(
        "[MVStoreFileQueue] MVStoreFileQueueProperties.queueName cannot be null or empty"));
  }
}