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
   * FileQueueException은 비즈니스 예외이므로 재시도 없이 즉시 전파된다.
   * 예외 메시지에 "Queue is full: size 0 >= maxSize 0"이 포함되어야 한다.
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
        assertThat(e.getMessage()).contains("Queue is full: size 0 >= maxSize 0");
        assertThat(e.getCause()).isNull();
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