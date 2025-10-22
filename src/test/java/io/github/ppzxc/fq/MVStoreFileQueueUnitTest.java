package io.github.ppzxc.fq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MVStoreFileQueueUnitTest {

  @DisplayName("set auto commit disabled")
  @Test
  void t0() {
    // given
    MVStoreFileQueueProperties mvStoreFileQueueProperties = new MVStoreFileQueueProperties();
    mvStoreFileQueueProperties.setFileName(String.format("test_queue_%d.db", System.currentTimeMillis()));
    mvStoreFileQueueProperties.setAutoCommitDisabled(true);

    // when
    FileQueue<String> fileQueue = FileQueueFactory.createMVStoreFileQueue(mvStoreFileQueueProperties);

    // then
    fileQueue.close();
  }

  @DisplayName("throw Queue is full exception")
  @Test
  void t1() {
    // given
    MVStoreFileQueueProperties mvStoreFileQueueProperties = new MVStoreFileQueueProperties();
    mvStoreFileQueueProperties.setFileName(String.format("test_queue_%d.db", System.currentTimeMillis()));
    mvStoreFileQueueProperties.setMaxSize(0);

    // when
    FileQueue<String> fileQueue = FileQueueFactory.createMVStoreFileQueue(mvStoreFileQueueProperties);

    // then
    assertThatCode(() -> fileQueue.enqueue("ITEM"))
        .isInstanceOfSatisfying(FileQueueException.class, e -> {
          assertThat(e.getMessage()).isEqualTo("Failed to acquire write lock after 3 attempts");
          assertThat(e.getCause()).isInstanceOf(FileQueueException.class);
          assertThat(e.getCause().getMessage()).isEqualTo("Queue is full: 0 > 0");
        });
  }

  @DisplayName("dequeue throw exception if empty")
  @Test
  void t2() {
    // given
    MVStoreFileQueueProperties mvStoreFileQueueProperties = new MVStoreFileQueueProperties();
    mvStoreFileQueueProperties.setFileName(String.format("test_queue_%d.db", System.currentTimeMillis()));

    // when
    FileQueue<String> fileQueue = FileQueueFactory.createMVStoreFileQueue(mvStoreFileQueueProperties);
    Optional<String> actual = fileQueue.dequeue();

    // then
    assertThat(actual).isEmpty();
  }
}