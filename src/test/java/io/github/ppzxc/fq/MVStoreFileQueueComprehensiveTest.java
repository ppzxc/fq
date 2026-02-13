package io.github.ppzxc.fq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MVStoreFileQueueComprehensiveTest {

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
  }

  @DisplayName("enqueue - null value throws exception")
  @Test
  void enqueue_nullValue() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // when, then
    assertThatThrownBy(() -> queue.enqueue((String) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("[MVStoreFileQueue] value cannot be null");
  }

  @DisplayName("enqueue list - null list throws exception")
  @Test
  void enqueueList_nullList() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // when, then
    assertThatThrownBy(() -> queue.enqueue((List<String>) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("[MVStoreFileQueue] value list cannot be null");
  }

  @DisplayName("enqueue list - empty list returns true")
  @Test
  void enqueueList_emptyList() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // when
    boolean result = queue.enqueue(Collections.emptyList());

    // then
    assertThat(result).isTrue();
    assertThat(queue.size()).isEqualTo(0);
  }

  @DisplayName("enqueue list - list with null element throws exception")
  @Test
  void enqueueList_listWithNullElement() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // when, then
    assertThatThrownBy(() -> queue.enqueue(Arrays.asList("item1", null, "item3")))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("[MVStoreFileQueue] value in list cannot be null");
  }

  @DisplayName("enqueue list - success")
  @Test
  void enqueueList_success() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
    List<String> items = Arrays.asList("item1", "item2", "item3");

    // when
    boolean result = queue.enqueue(items);

    // then
    assertThat(result).isTrue();
    assertThat(queue.size()).isEqualTo(3);
  }

  @DisplayName("dequeue with size - negative size throws exception")
  @Test
  void dequeueWithSize_negativeSize() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // when, then
    assertThatThrownBy(() -> queue.dequeue(-1))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("[MVStoreFileQueue] size cannot be negative");
  }

  @DisplayName("dequeue with size - zero size returns empty list")
  @Test
  void dequeueWithSize_zeroSize() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
    queue.enqueue("item1");

    // when
    List<String> result = queue.dequeue(0);

    // then
    assertThat(result).isEmpty();
    assertThat(queue.size()).isEqualTo(1);
  }

  @DisplayName("dequeue with size - size larger than queue size")
  @Test
  void dequeueWithSize_sizeLargerThanQueue() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
    queue.enqueue(Arrays.asList("item1", "item2"));

    // when
    List<String> result = queue.dequeue(5);

    // then
    assertThat(result).hasSize(2);
    assertThat(result).containsExactly("item1", "item2");
    assertThat(queue.isEmpty()).isTrue();
  }

  @DisplayName("close - idempotent close")
  @Test
  void close_idempotent() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // when
    queue.close();
    queue.close(); // second close should not throw

    // then - no exception
  }

  @DisplayName("operations after close throw exception - enqueue")
  @Test
  void afterClose_enqueue() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
    queue.close();

    // when, then
    assertThatThrownBy(() -> queue.enqueue("item"))
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("[MVStoreFileQueue] Queue is already closed");
  }

  @DisplayName("operations after close throw exception - enqueue list")
  @Test
  void afterClose_enqueueList() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
    queue.close();

    // when, then
    assertThatThrownBy(() -> queue.enqueue(Arrays.asList("item")))
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("[MVStoreFileQueue] Queue is already closed");
  }

  @DisplayName("operations after close throw exception - dequeue")
  @Test
  void afterClose_dequeue() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
    queue.close();

    // when, then
    assertThatThrownBy(() -> queue.dequeue())
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("[MVStoreFileQueue] Queue is already closed");
  }

  @DisplayName("operations after close throw exception - dequeue with size")
  @Test
  void afterClose_dequeueWithSize() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
    queue.close();

    // when, then
    assertThatThrownBy(() -> queue.dequeue(1))
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("[MVStoreFileQueue] Queue is already closed");
  }

  @DisplayName("operations after close throw exception - isEmpty")
  @Test
  void afterClose_isEmpty() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
    queue.close();

    // when, then
    assertThatThrownBy(() -> queue.isEmpty())
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("[MVStoreFileQueue] Queue is already closed");
  }

  @DisplayName("operations after close throw exception - size")
  @Test
  void afterClose_size() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
    queue.close();

    // when, then
    assertThatThrownBy(() -> queue.size())
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("[MVStoreFileQueue] Queue is already closed");
  }

  @DisplayName("operations after close throw exception - compactFile")
  @Test
  void afterClose_compactFile() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
    queue.close();

    // when, then
    assertThatThrownBy(() -> queue.compactFile())
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("[MVStoreFileQueue] Queue is already closed");
  }

  @DisplayName("fileName returns correct file name")
  @Test
  void fileName_returnsCorrectName() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // when
    String fileName = queue.fileName();

    // then
    assertThat(fileName).isEqualTo(path);
  }

  @DisplayName("metric logs without exception")
  @Test
  void metric_logsWithoutException() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
    queue.enqueue("item1");

    // when, then - should not throw
    queue.metric("test");
  }

  @DisplayName("compactFile - file size below threshold")
  @Test
  void compactFile_belowThreshold() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setCompactByFileSize(100 * 1024 * 1024); // 100MB
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
    queue.enqueue("item1");

    // when, then - should not throw
    queue.compactFile();
  }

  @DisplayName("compactFile - file size above threshold")
  @Test
  void compactFile_aboveThreshold() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setCompactByFileSize(1); // 1 byte - always trigger compact
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
    queue.enqueue("item1");

    // when, then - should not throw
    queue.compactFile();
  }

  @DisplayName("queue with useCompress false")
  @Test
  void queue_withoutCompression() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setUseCompress(false);
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // when
    queue.enqueue("item1");
    String result = queue.dequeue();

    // then
    assertThat(result).isEqualTo("item1");
  }

  @DisplayName("queue with fair lock false")
  @Test
  void queue_withUnfairLock() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setFair(false);
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // when
    queue.enqueue("item1");
    String result = queue.dequeue();

    // then
    assertThat(result).isEqualTo("item1");
  }

  @DisplayName("persistence - reopen queue and read data")
  @Test
  void persistence_reopenQueue() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
    queue.enqueue(Arrays.asList("item1", "item2", "item3"));
    queue.close();

    // when - reopen
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // then
    assertThat(queue.size()).isEqualTo(3);
    assertThat(queue.dequeue()).isEqualTo("item1");
    assertThat(queue.dequeue()).isEqualTo("item2");
    assertThat(queue.dequeue()).isEqualTo("item3");
  }

  @DisplayName("persistence - partial dequeue and reopen")
  @Test
  void persistence_partialDequeueAndReopen() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("test_queue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
    queue.enqueue(Arrays.asList("item1", "item2", "item3"));
    queue.dequeue(); // remove item1
    queue.close();

    // when - reopen
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // then
    assertThat(queue.size()).isEqualTo(2);
    assertThat(queue.dequeue()).isEqualTo("item2");
    assertThat(queue.dequeue()).isEqualTo("item3");
  }

  @DisplayName("FileQueueException - has correct constructors")
  @Test
  void fileQueueException_constructors() {
    // when
    FileQueueException ex1 = new FileQueueException("message");
    FileQueueException ex2 = new FileQueueException("message", new RuntimeException("cause"));

    // then
    assertThat(ex1.getMessage()).isEqualTo("message");
    assertThat(ex1.getCause()).isNull();
    assertThat(ex2.getMessage()).isEqualTo("message");
    assertThat(ex2.getCause()).isInstanceOf(RuntimeException.class);
    assertThat(ex2.getCause().getMessage()).isEqualTo("cause");
  }
}
