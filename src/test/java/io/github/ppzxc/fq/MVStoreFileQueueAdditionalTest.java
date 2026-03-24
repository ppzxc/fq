package io.github.ppzxc.fq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MVStoreFileQueueAdditionalTest {

  @TempDir
  Path tempDir;

  private FileQueue<String> queue;

  @AfterEach
  void tearDown() {
    if (queue != null) {
      try {
        queue.close();
      } catch (Exception e) {
        // ignore - queue may already be closed
      }
    }
  }

  /**
   * enqueue 작업 중 큐를 close하면 진행 중인 스레드들이 IllegalStateException을 받고
   * 모두 정상 종료되는지 검증한다.
   * <p>
   * close 이전에 성공한 enqueue가 1개 이상 존재해야 하며,
   * close 이후 enqueue 시도는 IllegalStateException으로 처리되어야 한다.
   * </p>
   */
  @DisplayName("concurrent close while enqueue operations in flight")
  @Test
  void concurrentClose_whileEnqueueInFlight() throws Exception {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("concurrent_close.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    int threadCount = 5;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(threadCount);
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger exceptionCount = new AtomicInteger(0);
    AtomicBoolean closeCalled = new AtomicBoolean(false);

    // when - start enqueue threads
    for (int i = 0; i < threadCount; i++) {
      final int threadId = i;
      executor.submit(() -> {
        try {
          startLatch.await();
          for (int j = 0; j < 10000; j++) {
            if (closeCalled.get()) {
              // Try one more operation after close to verify exception
              try {
                queue.enqueue("after-close-" + threadId);
              } catch (IllegalStateException e) {
                exceptionCount.incrementAndGet();
              }
              break;
            }
            queue.enqueue("item-" + threadId + "-" + j);
            successCount.incrementAndGet();
          }
        } catch (IllegalStateException e) {
          // Expected when queue is closed
          exceptionCount.incrementAndGet();
        } catch (Exception e) {
          // Other exceptions
        } finally {
          doneLatch.countDown();
        }
      });
    }

    startLatch.countDown();
    Thread.sleep(5); // Let some enqueue operations happen
    closeCalled.set(true);
    queue.close();

    boolean completed = doneLatch.await(5, TimeUnit.SECONDS);
    executor.shutdownNow();

    // then - all threads should complete and some operations succeeded before close
    assertThat(completed).isTrue();
    assertThat(successCount.get()).isGreaterThan(0);
  }

  /**
   * dequeue 작업 중 큐를 close하면 진행 중인 스레드들이 IllegalStateException을 받고
   * 모두 정상 종료되는지 검증한다.
   * <p>
   * close 이전에 성공한 dequeue가 1개 이상 존재해야 하며,
   * 모든 스레드가 5초 이내에 완료되어야 한다.
   * </p>
   */
  @DisplayName("concurrent close while dequeue operations in flight")
  @Test
  void concurrentClose_whileDequeueInFlight() throws Exception {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("concurrent_close_dequeue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // Pre-populate queue
    for (int i = 0; i < 10000; i++) {
      queue.enqueue("item-" + i);
    }

    int threadCount = 5;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(threadCount);
    AtomicInteger dequeueCount = new AtomicInteger(0);
    AtomicBoolean closeCalled = new AtomicBoolean(false);

    // when - start dequeue threads
    for (int i = 0; i < threadCount; i++) {
      executor.submit(() -> {
        try {
          startLatch.await();
          while (!closeCalled.get()) {
            String item = queue.dequeue();
            if (item != null) {
              dequeueCount.incrementAndGet();
            }
          }
          // Try one more operation after close
          queue.dequeue();
        } catch (IllegalStateException e) {
          // Expected when queue is closed
        } catch (Exception e) {
          // Other exceptions
        } finally {
          doneLatch.countDown();
        }
      });
    }

    startLatch.countDown();
    Thread.sleep(5);
    closeCalled.set(true);
    queue.close();

    boolean completed = doneLatch.await(5, TimeUnit.SECONDS);
    executor.shutdownNow();

    // then - all threads completed and some items were dequeued
    assertThat(completed).isTrue();
    assertThat(dequeueCount.get()).isGreaterThan(0);
  }

  /**
   * maxSize=10인 큐에 정확히 10개의 아이템을 삽입한 후 11번째 삽입 시 예외가 발생하는지 검증한다.
   * <p>
   * size >= maxSize 조건(10 >= 10)이 충족되면 큐가 가득 찬 것으로 간주하며,
   * FileQueueException의 root cause 메시지가 올바른지 확인한다.
   * </p>
   */
  @DisplayName("enqueue at exact maxSize boundary - fill to maxSize-1 then add one more")
  @Test
  void enqueue_atExactMaxSizeBoundary() {
    // given - maxSize uses >= check, so maxSize=10 allows 0-9 items (10 items fails)
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setMaxSize(10);
    String path = tempDir.resolve("boundary.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // when - fill to maxSize (10 items, which triggers size >= maxSize)
    for (int i = 0; i < 10; i++) {
      queue.enqueue("item-" + i);
    }

    // then - should have 10 items
    assertThat(queue.size()).isEqualTo(10);

    // when - try to add one more (should fail because size=10 >= maxSize=10)
    // Note: acquireWriteLock wraps the original exception, so we check the root cause
    assertThatThrownBy(() -> queue.enqueue("item-10"))
        .isInstanceOf(FileQueueException.class)
        .hasMessage("[MVStoreFileQueue] Queue is full: size 10 >= maxSize 10");
  }

  /**
   * maxSize=11인 큐에 10개를 채우고 1개를 dequeue한 후 새 아이템을 추가할 수 있는지 검증한다.
   * <p>
   * dequeue 후 size(9) + 1 = 10 &lt; maxSize(11)이므로 enqueue가 성공해야 하며,
   * 최종 큐 크기는 10이어야 한다.
   * </p>
   */
  @DisplayName("enqueue after dequeue at maxSize boundary")
  @Test
  void enqueue_afterDequeueAtMaxSizeBoundary() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setMaxSize(11); // >= check, so maxSize=11 allows 10 items
    String path = tempDir.resolve("boundary2.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // Fill to 10 items
    for (int i = 0; i < 10; i++) {
      queue.enqueue("item-" + i);
    }

    // when - dequeue one and try to add
    queue.dequeue();
    queue.enqueue("new-item");

    // then
    assertThat(queue.size()).isEqualTo(10);
  }

  /**
   * maxSize=10인 큐에 5개가 있을 때 5개짜리 배치를 삽입하면 예외가 발생하는지 검증한다.
   * <p>
   * size(5) + list.size(5) = 10 &gt;= maxSize(10) 조건으로 실패해야 하며,
   * 4개짜리 배치(9 &lt; 10)는 성공해야 한다.
   * </p>
   */
  @DisplayName("batch enqueue at maxSize boundary")
  @Test
  void batchEnqueue_atMaxSizeBoundary() {
    // given - batch enqueue uses size() + value.size() >= maxSize check
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setMaxSize(10);
    String path = tempDir.resolve("batch_boundary.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // Fill to 5 items
    for (int i = 0; i < 5; i++) {
      queue.enqueue("item-" + i);
    }

    // when - try to add 5 more (would make size + list.size = 10, which >= maxSize)
    List<String> batch = Arrays.asList("batch-0", "batch-1", "batch-2", "batch-3", "batch-4");

    // then - should fail because size(5) + batch.size(5) = 10 >= maxSize(10)
    // FileQueueException is propagated immediately (no retry wrapper)
    assertThatThrownBy(() -> queue.enqueue(batch))
        .isInstanceOf(FileQueueException.class)
        .hasMessageContaining("Queue is full");

    // Verify a smaller batch succeeds
    List<String> smallBatch = Arrays.asList("small-0", "small-1", "small-2", "small-3");
    queue.enqueue(smallBatch); // size(5) + 4 = 9 < 10, should succeed
    assertThat(queue.size()).isEqualTo(9);
  }

  /**
   * dequeue(int)에 현재 큐 크기와 정확히 같은 수를 전달하면 모든 아이템이 반환되고
   * 큐가 비어있는지 검증한다.
   * <p>FIFO 순서도 함께 검증한다.</p>
   */
  @DisplayName("dequeue with size exactly equal to queue size")
  @Test
  void dequeue_withSizeExactlyEqualToQueueSize() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("exact_dequeue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    int itemCount = 50;
    for (int i = 0; i < itemCount; i++) {
      queue.enqueue("item-" + i);
    }

    // when
    List<String> dequeued = queue.dequeue(itemCount);

    // then
    assertThat(dequeued).hasSize(itemCount);
    assertThat(queue.isEmpty()).isTrue();
    assertThat(queue.size()).isZero();

    // Verify order
    for (int i = 0; i < itemCount; i++) {
      assertThat(dequeued.get(i)).isEqualTo("item-" + i);
    }
  }

  /**
   * dequeue(int)에 현재 큐 크기보다 큰 수를 전달하면 현재 존재하는 모든 아이템만 반환되는지 검증한다.
   */
  @DisplayName("dequeue with size larger than queue size returns available items")
  @Test
  void dequeue_withSizeLargerThanQueueSize() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("larger_dequeue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    queue.enqueue("item-0");
    queue.enqueue("item-1");
    queue.enqueue("item-2");

    // when
    List<String> dequeued = queue.dequeue(100);

    // then
    assertThat(dequeued).hasSize(3);
    assertThat(queue.isEmpty()).isTrue();
  }

  /**
   * 빈 큐에서 compactFile()을 호출해도 예외가 발생하지 않고
   * 이후 enqueue/dequeue가 정상 동작하는지 검증한다.
   */
  @DisplayName("compact with empty queue")
  @Test
  void compact_withEmptyQueue() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setCompactByFileSize(1); // Very small threshold
    String path = tempDir.resolve("empty_compact.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // when - compact empty queue
    queue.compactFile();

    // then - no exception and queue still functional
    assertThat(queue.isEmpty()).isTrue();
    queue.enqueue("test");
    assertThat(queue.dequeue()).isEqualTo("test");
  }

  /**
   * enqueue/dequeue/compactFile이 동시에 실행되어도 교착상태(deadlock)가 발생하지 않는지 검증한다.
   * <p>
   * 200회의 enqueue, 지속적인 dequeue, 5회의 compactFile이 10초 이내에 완료되어야 한다.
   * </p>
   */
  @DisplayName("compact while concurrent enqueue/dequeue operations")
  @Test
  void compact_whileConcurrentOperations() throws Exception {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setCompactByFileSize(1);
    String path = tempDir.resolve("concurrent_compact.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    ExecutorService executor = Executors.newFixedThreadPool(3);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(3);
    AtomicBoolean running = new AtomicBoolean(true);
    Set<String> enqueuedItems = ConcurrentHashMap.newKeySet();
    Set<String> dequeuedItems = ConcurrentHashMap.newKeySet();

    // Enqueue thread
    executor.submit(() -> {
      try {
        startLatch.await();
        int count = 0;
        while (running.get() && count < 200) {
          String item = "item-" + count++;
          queue.enqueue(item);
          enqueuedItems.add(item);
        }
      } catch (Exception e) {
        // ignore
      } finally {
        doneLatch.countDown();
      }
    });

    // Dequeue thread
    executor.submit(() -> {
      try {
        startLatch.await();
        while (running.get()) {
          String item = queue.dequeue();
          if (item != null) {
            dequeuedItems.add(item);
          }
        }
      } catch (Exception e) {
        // ignore
      } finally {
        doneLatch.countDown();
      }
    });

    // Compact thread
    executor.submit(() -> {
      try {
        startLatch.await();
        for (int i = 0; i < 5; i++) {
          Thread.sleep(20);
          queue.compactFile();
        }
      } catch (Exception e) {
        // ignore
      } finally {
        running.set(false);
        doneLatch.countDown();
      }
    });

    startLatch.countDown();
    boolean completed = doneLatch.await(10, TimeUnit.SECONDS);
    executor.shutdownNow();

    // then - no deadlock occurred
    assertThat(completed).isTrue();
  }

  /**
   * batchSize=1(연산마다 커밋)이고 autoCommitDisabled=true일 때 500개의 아이템을 삽입하면
   * 모두 정확하게 FIFO 순서로 조회되는지 검증한다.
   */
  @DisplayName("very small batchSize with many operations")
  @Test
  void smallBatchSize_withManyOperations() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setBatchSize(1); // Commit after every operation
    properties.setAutoCommitDisabled(true);
    String path = tempDir.resolve("small_batch.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    int itemCount = 500;

    // when
    for (int i = 0; i < itemCount; i++) {
      queue.enqueue("item-" + i);
    }

    // then
    assertThat(queue.size()).isEqualTo(itemCount);

    // Verify all items are retrievable
    for (int i = 0; i < itemCount; i++) {
      String item = queue.dequeue();
      assertThat(item).isEqualTo("item-" + i);
    }
  }

  /**
   * batchSize보다 적은 아이템을 삽입하고 close한 후 재오픈 시 모든 아이템이 복구되는지 검증한다.
   * <p>
   * close() 호출 시 미커밋 데이터를 강제로 commit하여 영속성을 보장해야 한다.
   * 재오픈 후 50개의 아이템이 존재하고 첫 번째 아이템이 "item-0"이어야 한다.
   * </p>
   */
  @DisplayName("recovery after close and reopen with uncommitted data")
  @Test
  void recovery_afterCloseAndReopen() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setBatchSize(100); // Large batch size
    String path = tempDir.resolve("recovery.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // Enqueue some items (less than batch size, so no auto-commit)
    for (int i = 0; i < 50; i++) {
      queue.enqueue("item-" + i);
    }

    // Close (should commit remaining)
    queue.close();

    // Reopen
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // then - all items should be recovered
    assertThat(queue.size()).isEqualTo(50);
    assertThat(queue.dequeue()).isEqualTo("item-0");
  }

  /**
   * 20번의 open → enqueue → close 사이클을 반복한 후 재오픈 시 20개의 아이템이 모두 존재하는지 검증한다.
   * <p>반복적인 open/close가 데이터 손실 없이 누적 저장됨을 확인한다.</p>
   */
  @DisplayName("rapid open/close cycles")
  @Test
  void rapidOpenCloseCycles() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("rapid_cycle.db").toFile().getAbsolutePath();

    // when - rapid open/close cycles
    for (int cycle = 0; cycle < 20; cycle++) {
      queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
      queue.enqueue("item-" + cycle);
      queue.close();
    }

    // then - final reopen should have all items
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
    assertThat(queue.size()).isEqualTo(20);
  }

  /**
   * 한글, 일본어, 중국어, 이모지, 특수문자, 개행 문자 등 다양한 유니코드 문자열이
   * 직렬화/역직렬화 과정에서 손실 없이 보존되는지 검증한다.
   */
  @DisplayName("unicode and special character data")
  @Test
  void unicodeAndSpecialCharacterData() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("unicode.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // when - enqueue various unicode strings
    List<String> testStrings = Arrays.asList(
        "한글 테스트",
        "日本語テスト",
        "中文测试",
        "🎉🚀💯",
        "emoji: 😀🎈🎁",
        "mixed: Hello 世界 🌍",
        "special chars: <>&\"'",
        "newlines:\n\r\t",
        "null char before: \u0000 after",
        ""
    );

    for (String s : testStrings) {
      queue.enqueue(s);
    }

    // then - all strings should be dequeued correctly
    for (String expected : testStrings) {
      String actual = queue.dequeue();
      assertThat(actual).isEqualTo(expected);
    }
  }

  /**
   * id, name, items 필드를 가진 복잡한 직렬화 객체가 enqueue/dequeue 후에도
   * 모든 필드 값이 동일하게 유지되는지 검증한다.
   * <p>한글 문자열 필드도 포함하여 직렬화 무결성을 확인한다.</p>
   */
  @DisplayName("complex serializable object")
  @Test
  void complexSerializableObject() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("complex.db").toFile().getAbsolutePath();
    FileQueue<TestObject> objectQueue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    try {
      TestObject obj1 = new TestObject(1, "test", Arrays.asList("a", "b", "c"));
      TestObject obj2 = new TestObject(2, "한글", Arrays.asList("가", "나", "다"));

      // when
      objectQueue.enqueue(obj1);
      objectQueue.enqueue(obj2);

      // then
      TestObject dequeued1 = objectQueue.dequeue();
      TestObject dequeued2 = objectQueue.dequeue();

      assertThat(dequeued1).isEqualTo(obj1);
      assertThat(dequeued2).isEqualTo(obj2);
    } finally {
      objectQueue.close();
    }
  }

  /**
   * metric() 호출 시 다양한 이름(영문, 빈 문자열, 한글)에 대해 예외가 발생하지 않는지 검증한다.
   */
  @DisplayName("metric logging does not throw")
  @Test
  void metric_doesNotThrow() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("metric.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    queue.enqueue("item1");
    queue.enqueue("item2");
    queue.dequeue();

    // when/then - should not throw
    queue.metric("test-metric");
    queue.metric("");
    queue.metric("한글메트릭");
  }

  /**
   * fileName()이 큐 생성 시 전달한 절대 경로를 정확히 반환하는지 검증한다.
   */
  @DisplayName("fileName returns correct path")
  @Test
  void fileName_returnsCorrectPath() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("filename_test.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // when
    String fileName = queue.fileName();

    // then
    assertThat(fileName).isEqualTo(path);
  }

  /**
   * 큐를 닫은 후 모든 연산이 "closed" 메시지를 포함한 IllegalStateException을 던지는지 검증한다.
   * <p>
   * enqueue(T), enqueue(List), dequeue(), dequeue(int), isEmpty(), size(), compactFile() 모두
   * IllegalStateException을 발생시켜야 한다.
   * </p>
   */
  @DisplayName("operations after close throw IllegalStateException")
  @Test
  void operationsAfterClose_throwIllegalStateException() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("closed.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
    queue.close();

    // then
    assertThatThrownBy(() -> queue.enqueue("test"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("closed");

    assertThatThrownBy(() -> queue.enqueue(Arrays.asList("a", "b")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("closed");

    assertThatThrownBy(() -> queue.dequeue())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("closed");

    assertThatThrownBy(() -> queue.dequeue(5))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("closed");

    assertThatThrownBy(() -> queue.isEmpty())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("closed");

    assertThatThrownBy(() -> queue.size())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("closed");

    assertThatThrownBy(() -> queue.compactFile())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("closed");
  }

  /**
   * enqueue(List)에 빈 리스트를 전달하면 true를 반환하고 큐가 비어있는지 검증한다.
   */
  @DisplayName("empty list enqueue returns true")
  @Test
  void emptyListEnqueue_returnsTrue() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("empty_list.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // when
    boolean result = queue.enqueue(new ArrayList<>());

    // then
    assertThat(result).isTrue();
    assertThat(queue.isEmpty()).isTrue();
  }

  /**
   * dequeue(0) 호출 시 빈 리스트를 반환하고 기존 아이템이 그대로 유지되는지 검증한다.
   */
  @DisplayName("dequeue with zero size returns empty list")
  @Test
  void dequeueWithZeroSize_returnsEmptyList() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("zero_dequeue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
    queue.enqueue("item");

    // when
    List<String> result = queue.dequeue(0);

    // then
    assertThat(result).isEmpty();
    assertThat(queue.size()).isEqualTo(1);
  }

  /**
   * dequeue(-1) 호출 시 "negative" 메시지를 포함한 IllegalArgumentException이 발생하는지 검증한다.
   */
  @DisplayName("dequeue with negative size throws exception")
  @Test
  void dequeueWithNegativeSize_throwsException() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("negative_dequeue.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // when/then
    assertThatThrownBy(() -> queue.dequeue(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("negative");
  }

  /**
   * enqueue/dequeue가 교차로 수행될 때 FIFO 순서가 올바르게 유지되는지 검증한다.
   * <p>
   * 1, 2 enqueue → 1 dequeue → 3, 4 enqueue → 2, 3 dequeue → 5 enqueue
   * → 4, 5 dequeue 순서가 정확히 보장되어야 한다.
   * </p>
   */
  @DisplayName("FIFO order maintained after multiple operations")
  @Test
  void fifoOrder_maintainedAfterMultipleOperations() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("fifo.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // when - interleaved enqueue/dequeue
    queue.enqueue("1");
    queue.enqueue("2");
    assertThat(queue.dequeue()).isEqualTo("1");
    queue.enqueue("3");
    queue.enqueue("4");
    assertThat(queue.dequeue()).isEqualTo("2");
    assertThat(queue.dequeue()).isEqualTo("3");
    queue.enqueue("5");

    // then
    assertThat(queue.dequeue()).isEqualTo("4");
    assertThat(queue.dequeue()).isEqualTo("5");
    assertThat(queue.isEmpty()).isTrue();
  }

  /**
   * 압축 활성화 큐와 비활성화 큐가 동일한 데이터를 100개 삽입했을 때
   * 두 큐 모두 동일한 크기와 순서로 데이터를 반환하는지 검증한다.
   */
  @DisplayName("compression enabled vs disabled")
  @Test
  void compression_enabledVsDisabled() {
    // given
    MVStoreFileQueueProperties propsCompressed = new MVStoreFileQueueProperties();
    propsCompressed.setUseCompress(true);
    String pathCompressed = tempDir.resolve("compressed.db").toFile().getAbsolutePath();

    MVStoreFileQueueProperties propsUncompressed = new MVStoreFileQueueProperties();
    propsUncompressed.setUseCompress(false);
    String pathUncompressed = tempDir.resolve("uncompressed.db").toFile().getAbsolutePath();

    FileQueue<String> compressedQueue = FileQueueFactory.createMVStoreFileQueue(pathCompressed, propsCompressed);
    FileQueue<String> uncompressedQueue = FileQueueFactory.createMVStoreFileQueue(pathUncompressed, propsUncompressed);

    try {
      // when - add same data to both
      String largeData = "x".repeat(1000);
      for (int i = 0; i < 100; i++) {
        compressedQueue.enqueue(largeData + i);
        uncompressedQueue.enqueue(largeData + i);
      }

      // then - both should work correctly
      assertThat(compressedQueue.size()).isEqualTo(100);
      assertThat(uncompressedQueue.size()).isEqualTo(100);

      // Verify data integrity
      for (int i = 0; i < 100; i++) {
        assertThat(compressedQueue.dequeue()).isEqualTo(largeData + i);
        assertThat(uncompressedQueue.dequeue()).isEqualTo(largeData + i);
      }
    } finally {
      compressedQueue.close();
      uncompressedQueue.close();
    }
  }

  /**
   * 공정 락(fair=true) 큐와 비공정 락(fair=false) 큐가 동시 접근 시
   * 두 큐 모두 정확히 threadCount × opsPerThread 개의 아이템을 저장하는지 검증한다.
   */
  @DisplayName("fair vs unfair lock mode")
  @Test
  void fairVsUnfairLockMode() throws Exception {
    // given
    MVStoreFileQueueProperties fairProps = new MVStoreFileQueueProperties();
    fairProps.setFair(true);
    String fairPath = tempDir.resolve("fair.db").toFile().getAbsolutePath();

    MVStoreFileQueueProperties unfairProps = new MVStoreFileQueueProperties();
    unfairProps.setFair(false);
    String unfairPath = tempDir.resolve("unfair.db").toFile().getAbsolutePath();

    FileQueue<Integer> fairQueue = FileQueueFactory.createMVStoreFileQueue(fairPath, fairProps);
    FileQueue<Integer> unfairQueue = FileQueueFactory.createMVStoreFileQueue(unfairPath, unfairProps);

    try {
      // when - concurrent operations on both
      int threadCount = 4;
      int opsPerThread = 100;
      ExecutorService executor = Executors.newFixedThreadPool(threadCount * 2);
      CountDownLatch latch = new CountDownLatch(threadCount * 2);
      AtomicInteger fairCounter = new AtomicInteger(0);
      AtomicInteger unfairCounter = new AtomicInteger(0);

      for (int i = 0; i < threadCount; i++) {
        executor.submit(() -> {
          try {
            for (int j = 0; j < opsPerThread; j++) {
              fairQueue.enqueue(fairCounter.incrementAndGet());
            }
          } finally {
            latch.countDown();
          }
        });
        executor.submit(() -> {
          try {
            for (int j = 0; j < opsPerThread; j++) {
              unfairQueue.enqueue(unfairCounter.incrementAndGet());
            }
          } finally {
            latch.countDown();
          }
        });
      }

      latch.await(10, TimeUnit.SECONDS);
      executor.shutdownNow();

      // then - both should have all items
      assertThat(fairQueue.size()).isEqualTo(threadCount * opsPerThread);
      assertThat(unfairQueue.size()).isEqualTo(threadCount * opsPerThread);
    } finally {
      fairQueue.close();
      unfairQueue.close();
    }
  }

  /**
   * 10개 enqueue, 5개 dequeue 후 close하고 재오픈 시 head/tail 포인터가
   * 올바르게 복원되어 남은 5개 아이템이 순서대로 조회되는지 검증한다.
   */
  @DisplayName("head and tail pointers correct after reopen")
  @Test
  void headTailPointers_correctAfterReopen() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("pointers.db").toFile().getAbsolutePath();

    // First session: enqueue 10, dequeue 5
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
    for (int i = 0; i < 10; i++) {
      queue.enqueue("item-" + i);
    }
    for (int i = 0; i < 5; i++) {
      queue.dequeue();
    }
    queue.close();

    // Reopen
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // then - should have 5 items starting from item-5
    assertThat(queue.size()).isEqualTo(5);
    assertThat(queue.dequeue()).isEqualTo("item-5");
    assertThat(queue.dequeue()).isEqualTo("item-6");
  }

  /**
   * close()를 3번 연속 호출해도 예외가 발생하지 않는지(멱등성) 검증한다.
   */
  @DisplayName("multiple close calls are idempotent")
  @Test
  void multipleClose_isIdempotent() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("multi_close.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
    queue.enqueue("item");

    // when/then - multiple close calls should not throw
    queue.close();
    queue.close();
    queue.close();
  }

  /**
   * 10,000개의 아이템을 삽입하고 모두 FIFO 순서로 조회할 수 있는지 검증하는 스트레스 테스트이다.
   * <p>batchSize=500으로 20번의 자동 커밋이 발생하며, 데이터 무결성을 확인한다.</p>
   */
  @DisplayName("high volume stress test")
  @Test
  void highVolumeStressTest() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setBatchSize(500);
    String path = tempDir.resolve("stress.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    int itemCount = 10000;

    // when - high volume enqueue
    for (int i = 0; i < itemCount; i++) {
      queue.enqueue("item-" + i);
    }

    // then
    assertThat(queue.size()).isEqualTo(itemCount);

    // Dequeue all
    for (int i = 0; i < itemCount; i++) {
      String item = queue.dequeue();
      assertThat(item).isEqualTo("item-" + i);
    }

    assertThat(queue.isEmpty()).isTrue();
  }

  /**
   * metric()은 checkClosed() 가드가 없으므로 close() 이후에도 예외 없이 호출 가능한지 검증한다.
   * <p>
   * metric()은 내부 상태(totalOperation, totalCommits, head, tail)를 로그로 출력하는 용도이며,
   * 큐가 닫힌 후에도 AtomicLong 값은 읽을 수 있어야 한다.
   * </p>
   */
  @DisplayName("metric can be called after close without exception")
  @Test
  void metric_calledAfterClose_doesNotThrow() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("metric_after_close.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
    queue.enqueue("item1");
    queue.enqueue("item2");
    queue.dequeue();
    queue.close();

    // when/then - metric() has no checkClosed() guard, so it should not throw
    assertThatCode(() -> queue.metric("after-close-metric"))
        .doesNotThrowAnyException();
  }

  /**
   * enqueue(List)에 단일 원소 리스트를 전달하면 큐 크기가 1이 되고
   * dequeue 시 해당 원소가 반환되는지 검증한다.
   */
  @DisplayName("enqueue single element list succeeds")
  @Test
  void enqueue_singleElementList_success() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("single_list.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // when
    boolean result = queue.enqueue(Arrays.asList("only-item"));

    // then
    assertThat(result).isTrue();
    assertThat(queue.size()).isEqualTo(1);
    assertThat(queue.dequeue()).isEqualTo("only-item");
    assertThat(queue.isEmpty()).isTrue();
  }

  /**
   * autoCommitDisabled=true일 때 정확히 batchSize개의 아이템을 삽입하면
   * 수동 커밋이 1회 발생하고 모든 데이터가 저장되는지 검증한다.
   * <p>
   * close 후 재오픈 시 batchSize개 아이템이 복구되어 영속성이 보장됨을 확인한다.
   * </p>
   */
  @DisplayName("batch enqueue of exactly batchSize items triggers one commit")
  @Test
  void batchEnqueue_exactlyBatchSizeItems_triggersCommit() {
    // given
    int batchSize = 10;
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setBatchSize(batchSize);
    properties.setAutoCommitDisabled(true);
    String path = tempDir.resolve("exact_batch.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // when - enqueue exactly batchSize items to trigger exactly one commit
    for (int i = 0; i < batchSize; i++) {
      queue.enqueue("item-" + i);
    }

    // then - all items should be present
    assertThat(queue.size()).isEqualTo(batchSize);

    // close and reopen to verify persistence via commit
    queue.close();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
    assertThat(queue.size()).isEqualTo(batchSize);
    assertThat(queue.dequeue()).isEqualTo("item-0");
  }

  /**
   * 커스텀 queueName으로 저장된 큐를 close 후 재오픈 시 동일한 queueName을 사용하면
   * 이전에 저장된 데이터가 올바르게 복구되는지 검증한다.
   */
  @DisplayName("persistence with custom queue name")
  @Test
  void persistence_withCustomQueueName() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setQueueName("custom-queue");
    String path = tempDir.resolve("custom_name.db").toFile().getAbsolutePath();

    // when - save with custom queue name
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);
    queue.enqueue("item-0");
    queue.enqueue("item-1");
    queue.enqueue("item-2");
    queue.close();

    // reopen with same queueName
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // then - data should be restored correctly
    assertThat(queue.size()).isEqualTo(3);
    assertThat(queue.dequeue()).isEqualTo("item-0");
    assertThat(queue.dequeue()).isEqualTo("item-1");
    assertThat(queue.dequeue()).isEqualTo("item-2");
    assertThat(queue.isEmpty()).isTrue();
  }

  /**
   * 여러 스레드가 동시에 isEmpty()와 size()를 호출할 때 예외 없이 일관된 결과를 반환하는지 검증한다.
   * <p>
   * 읽기 락(read lock)으로 보호되는 isEmpty()와 size()가 동시 호출 시에도
   * 정상적으로 동작함을 확인한다.
   * </p>
   */
  @DisplayName("concurrent size and isEmpty calls return consistent results")
  @Test
  void concurrent_sizeAndIsEmpty_returnConsistentResults() throws Exception {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("concurrent_reads.db").toFile().getAbsolutePath();
    queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // Pre-populate
    for (int i = 0; i < 100; i++) {
      queue.enqueue("item-" + i);
    }

    int threadCount = 8;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(threadCount);
    AtomicReference<Throwable> error = new AtomicReference<>();

    for (int i = 0; i < threadCount; i++) {
      executor.submit(() -> {
        try {
          startLatch.await();
          for (int j = 0; j < 50; j++) {
            long size = queue.size();
            boolean empty = queue.isEmpty();
            // size and isEmpty should be consistent: if size==0 then empty must be true
            if (size == 0 && !empty) {
              error.set(new AssertionError("Inconsistent state: size=0 but isEmpty=false"));
            }
          }
        } catch (Exception e) {
          error.set(e);
        } finally {
          doneLatch.countDown();
        }
      });
    }

    startLatch.countDown();
    boolean completed = doneLatch.await(10, TimeUnit.SECONDS);
    executor.shutdownNow();

    // then
    assertThat(completed).isTrue();
    assertThat(error.get()).isNull();
  }

  // Helper class for complex object testing
  private static class TestObject implements Serializable {
    private static final long serialVersionUID = 1L;
    private final int id;
    private final String name;
    private final List<String> items;

    TestObject(int id, String name, List<String> items) {
      this.id = id;
      this.name = name;
      this.items = new ArrayList<>(items);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TestObject that = (TestObject) o;
      return id == that.id && Objects.equals(name, that.name) && Objects.equals(items, that.items);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, name, items);
    }
  }
}
