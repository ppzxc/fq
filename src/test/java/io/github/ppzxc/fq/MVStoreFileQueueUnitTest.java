package io.github.ppzxc.fq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MVStoreFileQueueUnitTest {

  @TempDir
  Path tempDir;

  @DisplayName("set auto commit disabled")
  @Test
  void t0() {
    // given
    MVStoreFileQueueProperties mvStoreFileQueueProperties = new MVStoreFileQueueProperties();
    mvStoreFileQueueProperties.setFileName(tempDir.resolve(String.format("test_queue_%d.db", System.currentTimeMillis())).toFile().getAbsolutePath());
    mvStoreFileQueueProperties.setAutoCommitDisabled(true);

    // when, then
    assertThatCode(() -> FileQueueFactory.createMVStoreFileQueue(mvStoreFileQueueProperties))
      .isNull();
  }

  @DisplayName("throw Queue is full exception")
  @Test
  void t1() {
    // given
    MVStoreFileQueueProperties mvStoreFileQueueProperties = new MVStoreFileQueueProperties();
    mvStoreFileQueueProperties.setFileName(tempDir.resolve(String.format("test_queue_%d.db", System.currentTimeMillis())).toFile().getAbsolutePath());
    mvStoreFileQueueProperties.setMaxSize(0);

    // when
    FileQueue<String> fileQueue = FileQueueFactory.createMVStoreFileQueue(mvStoreFileQueueProperties);

    // then
    assertThatCode(() -> fileQueue.enqueue("ITEM"))
      .isInstanceOfSatisfying(FileQueueException.class, e -> {
        assertThat(e.getMessage()).isEqualTo("[MVStoreFileQueue] Failed to acquire write lock after 3 attempts");
        assertThat(e.getCause()).isInstanceOf(FileQueueException.class);
        assertThat(e.getCause().getMessage()).isEqualTo("Queue is full: 0 > 0");
      });
  }

  @DisplayName("dequeue throw exception if empty")
  @Test
  void t2() {
    // given
    MVStoreFileQueueProperties mvStoreFileQueueProperties = new MVStoreFileQueueProperties();
    mvStoreFileQueueProperties.setFileName(tempDir.resolve(String.format("test_queue_%d.db", System.currentTimeMillis())).toFile().getAbsolutePath());

    // when
    FileQueue<String> fileQueue = FileQueueFactory.createMVStoreFileQueue(mvStoreFileQueueProperties);
    Optional<String> actual = fileQueue.dequeue();

    // then
    assertThat(actual).isEmpty();
  }
}