package io.github.ppzxc.fq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.nio.file.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MVStoreFileQueueUnitTest {

  @TempDir
  Path tempDir;

  /**
   * autoCommitDisabled=true 설정 시 큐가 정상적으로 초기화되는지 검증한다.
   * <p>FileQueueFactory를 통해 큐 생성 시 예외가 발생하지 않아야 한다.</p>
   */
  @DisplayName("set auto commit disabled")
  @Test
  void t0() {
    // given
    MVStoreFileQueueProperties mvStoreFileQueueProperties = new MVStoreFileQueueProperties();
    String absolutePath = tempDir.resolve(String.format("test_queue_%d.db", System.currentTimeMillis())).toFile()
      .getAbsolutePath();
    mvStoreFileQueueProperties.setAutoCommitDisabled(true);

    // when, then
    assertThatCode(() -> FileQueueFactory.createMVStoreFileQueue(absolutePath, mvStoreFileQueueProperties))
      .isNull();
  }

  /**
   * maxSize=0 설정된 큐에 아이템을 삽입하면 FileQueueException이 발생하는지 검증한다.
   * <p>
   * 예외는 "[MVStoreFileQueue] Failed to acquire write lock after 3 attempts" 메시지를 가지며,
   * 원인(cause)은 "[MVStoreFileQueue] Queue is full: 0 > 0" 메시지를 가져야 한다.
   * </p>
   */
  @DisplayName("throw Queue is full exception")
  @Test
  void t1() {
    // given
    MVStoreFileQueueProperties mvStoreFileQueueProperties = new MVStoreFileQueueProperties();
    String absolutePath = tempDir.resolve(String.format("test_queue_%d.db", System.currentTimeMillis())).toFile()
      .getAbsolutePath();
    mvStoreFileQueueProperties.setMaxSize(0);

    // when
    FileQueue<String> fileQueue = FileQueueFactory.createMVStoreFileQueue(absolutePath, mvStoreFileQueueProperties);

    // then
    assertThatCode(() -> fileQueue.enqueue("ITEM"))
      .isInstanceOfSatisfying(FileQueueException.class, e -> {
        assertThat(e.getMessage()).isEqualTo("[MVStoreFileQueue] Failed to execute with write lock after 3 attempts");
        assertThat(e.getCause()).isInstanceOf(FileQueueException.class);
        assertThat(e.getCause().getMessage()).isEqualTo("[MVStoreFileQueue] Queue is full: size 0 >= maxSize 0");
      });
  }

  /**
   * 빈 큐에서 dequeue() 호출 시 null을 반환하는지 검증한다.
   * <p>아이템이 없는 큐에서 dequeue()는 예외 없이 null을 반환해야 한다.</p>
   */
  @DisplayName("dequeue throw exception if empty")
  @Test
  void t2() {
    // given
    MVStoreFileQueueProperties mvStoreFileQueueProperties = new MVStoreFileQueueProperties();
    String absolutePath = tempDir.resolve(String.format("test_queue_%d.db", System.currentTimeMillis())).toFile()
      .getAbsolutePath();

    // when
    FileQueue<String> fileQueue = FileQueueFactory.createMVStoreFileQueue(absolutePath, mvStoreFileQueueProperties);
    String actual = fileQueue.dequeue();

    // then
    assertThat(actual).isNull();
  }
}