package io.github.ppzxc.fq;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MVStoreFileQueueDefaultTest {

  private static final String FILE_NAME = "test_queue.db";
  private FileQueue<String> fileQueue;

  @BeforeEach
  void setUp() {
    File file = new File(FILE_NAME);
    if (file.exists()) {
      file.delete();
    }
  }

  @AfterEach
  void tearDown() {
    if (fileQueue != null) {
      fileQueue.close();
    }
    File file = new File(FILE_NAME);
    if (file.exists()) {
      file.delete();
    }
  }

  @DisplayName("enqueue, dequeue test")
  @Test
  void t0() {
    // given
    MVStoreFileQueueProperties mvStoreFileQueueProperties = new MVStoreFileQueueProperties();
    mvStoreFileQueueProperties.setFileName(FILE_NAME);
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
}