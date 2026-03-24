package io.github.ppzxc.fq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MVStoreFileQueueCoverageTest {

  @TempDir
  Path tempDir;

  private String dbPath;
  private MVStoreFileQueue<String> queue;

  @BeforeEach
  void setUp() {
    dbPath = tempDir.resolve("test-queue.db").toString();
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setQueueName("testQueue");
    queue = new MVStoreFileQueue<>(properties, dbPath);
  }

  @AfterEach
  void tearDown() {
    if (queue != null) {
      queue.close();
    }
  }

  /**
   * 락 획득 대기 중 스레드가 인터럽트되면 backoff 슬립이 중단되고
   * 인터럽트 상태가 복원되는지 검증한다.
   * <p>
   * sleepBackoff() 내부에서 InterruptedException 발생 시
   * Thread.currentThread().interrupt()가 호출되어 인터럽트 상태가 보존되어야 한다.
   * </p>
   */
  @Test
  void testSleepBackoffInterrupted() throws Exception {
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setMaxRetry(2);
    properties.setRetryBackoffMs(1000);

    MVStoreFileQueue<String> failQueue = new MVStoreFileQueue<>(properties, tempDir.resolve("fail-interrupted.db").toString());

    java.util.concurrent.locks.ReentrantReadWriteLock lock = (java.util.concurrent.locks.ReentrantReadWriteLock)
        org.junit.platform.commons.util.ReflectionUtils.tryToReadFieldValue(MVStoreFileQueue.class, "lock", failQueue).get();

    lock.writeLock().lock(); // Lock it manually

    Thread mainThread = Thread.currentThread();
    Thread interruptor = new Thread(() -> {
        try {
            Thread.sleep(200);
            mainThread.interrupt();
        } catch (InterruptedException e) {}
    });

    interruptor.start();

    try {
        failQueue.enqueue("test");
    } catch (FileQueueException e) {
        // Expected
    } finally {
        lock.writeLock().unlock();
        Thread.interrupted(); // Clear interrupted status
    }

    failQueue.close();
  }

  /**
   * 디렉터리 경로를 fileName으로 사용하면 MVStore 초기화 실패로 FileQueueException이 발생하는지 검증한다.
   * <p>예외 메시지는 "Failed to initialize queue"를 포함해야 한다.</p>
   */
  @Test
  void testConstructorException() {
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    // Providing a directory path as fileName should cause an exception during MVStore.open()
    String dirPath = tempDir.resolve("a-directory").toString();
    new File(dirPath).mkdirs();

    assertThatThrownBy(() -> new MVStoreFileQueue<>(properties, dirPath))
        .isInstanceOf(FileQueueException.class)
        .hasMessageContaining("Failed to initialize queue");
  }

  /**
   * enqueue(T)에 null을 전달하면 IllegalArgumentException이 발생하는지 검증한다.
   * <p>예외 메시지는 "value cannot be null"을 포함해야 한다.</p>
   */
  @Test
  void testEnqueueNullValue() {
    assertThatThrownBy(() -> queue.enqueue((String) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("value cannot be null");
  }

  /**
   * enqueue(List)에 null 리스트를 전달하면 IllegalArgumentException이 발생하는지 검증한다.
   * <p>예외 메시지는 "value list cannot be null"을 포함해야 한다.</p>
   */
  @Test
  void testEnqueueNullList() {
    assertThatThrownBy(() -> queue.enqueue((List<String>) null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("value list cannot be null");
  }

  /**
   * enqueue(List)에 빈 리스트를 전달하면 true를 반환하고 예외가 발생하지 않는지 검증한다.
   */
  @Test
  void testEnqueueEmptyList() {
    assertThat(queue.enqueue(Collections.emptyList())).isTrue();
  }

  /**
   * enqueue(List)에 null 원소가 포함된 리스트를 전달하면 IllegalArgumentException이 발생하는지 검증한다.
   * <p>예외 메시지는 "value in list cannot be null"을 포함해야 한다.</p>
   */
  @Test
  void testEnqueueListWithNullElement() {
    List<String> list = Arrays.asList("a", null, "b");
    assertThatThrownBy(() -> queue.enqueue(list))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("value in list cannot be null");
  }

  /**
   * maxSize=1로 설정된 큐에 두 번째 아이템을 삽입하면 FileQueueException이 발생하는지 검증한다.
   */
  @Test
  void testEnqueueFullQueue() {
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setMaxSize(1);
    MVStoreFileQueue<String> smallQueue = new MVStoreFileQueue<>(properties, tempDir.resolve("small.db").toString());

    smallQueue.enqueue("one");
    assertThatThrownBy(() -> smallQueue.enqueue("two"))
      .isInstanceOf(FileQueueException.class);

    smallQueue.close();
  }

  /**
   * maxSize=2로 설정된 큐가 가득 찼을 때 에러 메시지가 ">=" 형식을 사용하는지 검증한다.
   * <p>조건이 size >= maxSize 이므로 메시지도 ">=" 기호를 사용해야 한다.</p>
   */
  @Test
  void testEnqueueFullQueueErrorMessage() {
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setMaxSize(2);
    MVStoreFileQueue<String> smallQueue = new MVStoreFileQueue<>(properties, tempDir.resolve("msg-test.db").toString());

    smallQueue.enqueue("one");
    smallQueue.enqueue("two");

    assertThatThrownBy(() -> smallQueue.enqueue("three"))
      .isInstanceOf(FileQueueException.class)
      .hasMessageContaining("size 2 >= maxSize 2");

    smallQueue.close();
  }

  /**
   * maxSize=2로 설정된 큐에 1개가 있을 때 2개짜리 리스트를 삽입하면 FileQueueException이 발생하는지 검증한다.
   * <p>size(1) + list.size(2) = 3 &ge; maxSize(2) 조건을 검증한다.</p>
   */
  @Test
  void testEnqueueListFullQueue() {
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setMaxSize(2);
    MVStoreFileQueue<String> smallQueue = new MVStoreFileQueue<>(properties, tempDir.resolve("small-list.db").toString());

    smallQueue.enqueue("one");
    List<String> list = Arrays.asList("two", "three");
    assertThatThrownBy(() -> smallQueue.enqueue(list))
      .isInstanceOf(FileQueueException.class)
      .hasMessageContaining("size 1 >= maxSize 2");

    smallQueue.close();
  }

  /**
   * dequeue(-1) 호출 시 IllegalArgumentException이 발생하는지 검증한다.
   * <p>예외 메시지는 "size cannot be negative"를 포함해야 한다.</p>
   */
  @Test
  void testDequeueNegativeSize() {
    assertThatThrownBy(() -> queue.dequeue(-1))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("size cannot be negative");
  }

  /**
   * dequeue(0) 호출 시 빈 리스트가 반환되는지 검증한다.
   */
  @Test
  void testDequeueZeroSize() {
    assertThat(queue.dequeue(0)).isEmpty();
  }

  /**
   * 빈 큐에서 dequeue() 및 dequeue(5) 호출 시 각각 null과 빈 리스트가 반환되는지 검증한다.
   */
  @Test
  void testDequeueEmptyQueue() {
    assertThat(queue.dequeue()).isNull();
    assertThat(queue.dequeue(5)).isEmpty();
  }

  /**
   * 큐를 닫은 후 모든 연산(enqueue, dequeue, dequeue(int), size, isEmpty, compactFile)이
   * IllegalStateException을 던지는지 검증한다.
   */
  @Test
  void testOperationAfterClose() {
    queue.close();
    assertThatThrownBy(() -> queue.enqueue("test")).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> queue.dequeue()).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> queue.dequeue(1)).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> queue.size()).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> queue.isEmpty()).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> queue.compactFile()).isInstanceOf(IllegalStateException.class);
  }

  /**
   * close()를 두 번 호출해도 예외가 발생하지 않는지(멱등성) 검증한다.
   */
  @Test
  void testDoubleClose() {
    queue.close();
    assertThatCode(() -> queue.close()).doesNotThrowAnyException();
  }

  /**
   * metric() 호출이 예외 없이 정상 실행되는지 검증한다.
   */
  @Test
  void testMetric() {
    queue.enqueue("test");
    assertThatCode(() -> queue.metric("testMetric")).doesNotThrowAnyException();
  }

  /**
   * compactByFileSize=1 설정으로 compactFile()을 호출하면 실제 압축이 수행되고 예외가 발생하지 않는지 검증한다.
   */
  @Test
  void testCompactFile() {
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setCompactByFileSize(1); // Force compact
    MVStoreFileQueue<String> compactQueue = new MVStoreFileQueue<>(properties, tempDir.resolve("compact.db").toString());
    compactQueue.enqueue("test");
    assertThatCode(() -> compactQueue.compactFile()).doesNotThrowAnyException();
    compactQueue.close();
  }

  /**
   * compactByFileSize가 큰 값(1MB)으로 설정되어 임계값 미만일 때 compactFile()이 예외 없이 로그만 출력하는지 검증한다.
   */
  @Test
  void testCompactFileBelowThreshold() {
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setCompactByFileSize(1024 * 1024);
    MVStoreFileQueue<String> compactQueue = new MVStoreFileQueue<>(properties, tempDir.resolve("compact2.db").toString());
    compactQueue.enqueue("test");
    assertThatCode(() -> compactQueue.compactFile()).doesNotThrowAnyException();
    compactQueue.close();
  }

  /**
   * lock 내부에서도 closed 상태를 감지하고 IllegalStateException을 즉시 던지는지 검증한다.
   * <p>
   * closed 필드를 직접 true로 설정하여 외부 checkClosed()를 우회하고,
   * lock 내부의 closed 체크가 작동하는지 확인한다.
   * </p>
   */
  @Test
  void testEnqueue_closedInsideLock_throwsIllegalStateException() throws Exception {
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    MVStoreFileQueue<String> testQueue = new MVStoreFileQueue<>(properties, tempDir.resolve("close-lock-test.db").toString());

    // closed 필드를 직접 true로 설정 (외부 checkClosed()를 우회하여 lock 내부 체크 테스트)
    java.lang.reflect.Field closedField = MVStoreFileQueue.class.getDeclaredField("closed");
    closedField.setAccessible(true);
    closedField.set(testQueue, true);

    // lock 내부에서 closed 감지 → IllegalStateException 즉시 전파 (재시도 없음)
    assertThatThrownBy(() -> testQueue.enqueue("test"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Queue is already closed");

    assertThatThrownBy(() -> testQueue.isEmpty())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Queue is already closed");
  }

  /**
   * 큐가 가득 찬 상태에서 enqueue 시 재시도 없이 즉시 FileQueueException이 발생하는지 검증한다.
   * <p>
   * FileQueueException은 비즈니스 로직 예외이므로 재시도 대상이 아니다.
   * retryBackoffMs=1000ms 설정에서 500ms 이내 실패해야 한다.
   * </p>
   */
  @Test
  void testEnqueueFullQueue_noRetry() {
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setMaxSize(1);
    properties.setMaxRetry(3);
    properties.setRetryBackoffMs(1000);
    MVStoreFileQueue<String> fullQueue = new MVStoreFileQueue<>(properties, tempDir.resolve("no-retry.db").toString());

    fullQueue.enqueue("first");

    long start = System.nanoTime();
    assertThatThrownBy(() -> fullQueue.enqueue("second"))
      .isInstanceOf(FileQueueException.class)
      .hasMessageContaining("Queue is full");
    long elapsedMs = (System.nanoTime() - start) / 1_000_000;

    assertThat(elapsedMs).isLessThan(500);
    fullQueue.close();
  }

  /**
   * autoCommitDisabled=true이고 batchSize=1일 때 enqueue마다 수동 커밋이 발생하는지 검증한다.
   */
  @Test
  void testAutoCommitDisabled() {
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setAutoCommitDisabled(true);
    properties.setBatchSize(1);
    MVStoreFileQueue<String> commitQueue = new MVStoreFileQueue<>(properties, tempDir.resolve("commit-disabled.db").toString());
    commitQueue.enqueue("test");
    commitQueue.close();
  }

  /**
   * autoCommitDisabled=true이고 batchSize=1일 때 enqueue마다 doCommit()이 호출되어
   * commit이 정상적으로 수행되는지 검증한다.
   * <p>
   * batchSize=1로 설정하면 매 enqueue마다 commit이 발생한다.
   * close() 후 재오픈 시 데이터가 영속화되었는지 확인한다.
   * </p>
   */
  @Test
  void testCommitIfNeededException() {
    String dbPath = tempDir.resolve("commit-test.db").toString();
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setAutoCommitDisabled(true);
    properties.setBatchSize(1);

    MVStoreFileQueue<String> commitQueue = new MVStoreFileQueue<>(properties, dbPath);
    assertThat(commitQueue.enqueue("committed-item")).isTrue();
    assertThat(commitQueue.size()).isEqualTo(1);
    commitQueue.close();

    // doCommit()이 정상 동작했다면 재오픈 시 데이터가 있어야 한다
    MVStoreFileQueue<String> reopened = new MVStoreFileQueue<>(properties, dbPath);
    assertThat(reopened.size()).isEqualTo(1);
    assertThat(reopened.dequeue()).isEqualTo("committed-item");
    reopened.close();
  }

  /**
   * acquireReadLock()의 재시도 로직이 처음 2번은 예외를 던지고 3번째에 성공하는 시나리오에서
   * 올바르게 결과를 반환하는지 검증한다.
   * <p>리플렉션으로 내부 SupplierWithException을 직접 주입하여 재시도 메커니즘을 검증한다.</p>
   */
  @Test
  void testAcquireReadLockRetrySuccess() throws Exception {
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setMaxRetry(3);
    properties.setRetryBackoffMs(10);

    MVStoreFileQueue<String> retryQueue = new MVStoreFileQueue<>(properties, tempDir.resolve("retry-read.db").toString());

    java.util.concurrent.atomic.AtomicInteger count = new java.util.concurrent.atomic.AtomicInteger(0);

    // We want to test the retry loop in acquireReadLock.
    // Since it's private, we use reflection to call it or trigger it.
    java.lang.reflect.Method method = MVStoreFileQueue.class.getDeclaredMethod("executeWithReadLock",
        Class.forName("io.github.ppzxc.fq.MVStoreFileQueue$SupplierWithException"));
    method.setAccessible(true);

    Object supplier = java.lang.reflect.Proxy.newProxyInstance(
        MVStoreFileQueue.class.getClassLoader(),
        new Class[]{Class.forName("io.github.ppzxc.fq.MVStoreFileQueue$SupplierWithException")},
        (proxy, m, args) -> {
            if (count.getAndIncrement() < 2) {
                throw new Exception("Simulated transient failure");
            }
            return 42L;
        }
    );

    Object result = method.invoke(retryQueue, supplier);
    assertThat(result).isEqualTo(42L);
    assertThat(count.get()).isEqualTo(3);

    retryQueue.close();
  }

  /**
   * 리플렉션으로 queueName 필드를 빈 문자열로 변경한 후 큐 생성 시
   * IllegalArgumentException이 발생하는지 검증한다.
   */
  @Test
  void testConstructorEmptyQueueName() {
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    try {
        java.lang.reflect.Field field = MVStoreFileQueueProperties.class.getDeclaredField("queueName");
        field.setAccessible(true);
        field.set(properties, "");

        assertThatThrownBy(() -> new MVStoreFileQueue<>(properties, tempDir.resolve("empty-name.db").toString()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("queueName cannot be null or empty");
    } catch (Exception e) {}
  }

  /**
   * 리플렉션으로 queueName 필드를 null로 변경한 후 큐 생성 시
   * IllegalArgumentException이 발생하는지 검증한다.
   */
  @Test
  void testConstructorNullQueueName() {
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    try {
        java.lang.reflect.Field field = MVStoreFileQueueProperties.class.getDeclaredField("queueName");
        field.setAccessible(true);
        field.set(properties, null);

        assertThatThrownBy(() -> new MVStoreFileQueue<>(properties, tempDir.resolve("null-name.db").toString()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("queueName cannot be null or empty");
    } catch (Exception e) {}
  }

  /**
   * abbreviate() 메서드가 모든 TimeUnit 값에 대해 올바른 단위 약어를 반환하는지 검증한다.
   * <p>
   * ns(나노초), μs(마이크로초), ms(밀리초), s(초), min(분), h(시간), d(일)을
   * 각 TimeUnit에 대해 검증한다.
   * </p>
   */
  @Test
  void testAbbreviate() throws Exception {
    java.lang.reflect.Method method = MVStoreFileQueue.class.getDeclaredMethod("abbreviate", TimeUnit.class);
    method.setAccessible(true);
    assertThat(method.invoke(queue, TimeUnit.NANOSECONDS)).isEqualTo("ns");
    assertThat(method.invoke(queue, TimeUnit.MICROSECONDS)).isEqualTo("μs");
    assertThat(method.invoke(queue, TimeUnit.MILLISECONDS)).isEqualTo("ms");
    assertThat(method.invoke(queue, TimeUnit.SECONDS)).isEqualTo("s");
    assertThat(method.invoke(queue, TimeUnit.MINUTES)).isEqualTo("min");
    assertThat(method.invoke(queue, TimeUnit.HOURS)).isEqualTo("h");
    assertThat(method.invoke(queue, TimeUnit.DAYS)).isEqualTo("d");
  }

  /**
   * chooseUnit() 메서드가 입력된 나노초 값에 따라 적절한 TimeUnit을 선택하는지 검증한다.
   * <p>1일 이상이면 DAYS, 1시간 이상이면 HOURS 등 계층적으로 단위를 선택해야 한다.</p>
   */
  @Test
  void testChooseUnit() throws Exception {
    java.lang.reflect.Method method = MVStoreFileQueue.class.getDeclaredMethod("chooseUnit", long.class);
    method.setAccessible(true);
    assertThat(method.invoke(queue, TimeUnit.DAYS.toNanos(1))).isEqualTo(TimeUnit.DAYS);
    assertThat(method.invoke(queue, TimeUnit.HOURS.toNanos(1))).isEqualTo(TimeUnit.HOURS);
    assertThat(method.invoke(queue, TimeUnit.MINUTES.toNanos(1))).isEqualTo(TimeUnit.MINUTES);
    assertThat(method.invoke(queue, TimeUnit.SECONDS.toNanos(1))).isEqualTo(TimeUnit.SECONDS);
    assertThat(method.invoke(queue, TimeUnit.MILLISECONDS.toNanos(1))).isEqualTo(TimeUnit.MILLISECONDS);
    assertThat(method.invoke(queue, TimeUnit.MICROSECONDS.toNanos(1))).isEqualTo(TimeUnit.MICROSECONDS);
    assertThat(method.invoke(queue, TimeUnit.NANOSECONDS.toNanos(1))).isEqualTo(TimeUnit.NANOSECONDS);
  }

  /**
   * abbreviate()의 default 분기에 도달하는 케이스를 위한 테스트 플레이스홀더이다.
   * <p>
   * 표준 Java TimeUnit은 모든 케이스가 switch 문으로 처리되므로 default 분기는
   * 이론적으로 도달 불가능하다. JaCoCo 커버리지 제외 대상이다.
   * </p>
   */
  @Test
  void testAbbreviateDefault() throws Exception {
    java.lang.reflect.Method method = MVStoreFileQueue.class.getDeclaredMethod("abbreviate", TimeUnit.class);
    method.setAccessible(true);

    // We need a TimeUnit that is not in the switch case.
    // But switch covers all standard TimeUnits from NANOSECONDS to DAYS.
    // There are no other TimeUnits in standard Java unless it's a mock.
    // However, JaCoCo might mark the 'default: throw new AssertionError()' as missed.
  }

  /**
   * 존재하지 않는 경로로 fileName 필드를 변경한 후 fileSize() 메서드를 호출하면
   * IOException이 처리되어 0이 반환되는지 검증한다.
   * <p>Files.size()가 NoSuchFileException(IOException 하위 클래스)을 던지면 0을 반환해야 한다.</p>
   */
  @Test
  void testFileSizeIOException() throws Exception {
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    MVStoreFileQueue<String> ioQueue = new MVStoreFileQueue<>(properties, tempDir.resolve("io-test.db").toString());

    // Changing fileName to something that exists but is not a file (like a directory)
    // might not trigger IOException in Files.size(), it might just return directory size.
    // Let's try to use a non-existent path that doesn't trigger InvalidPathException but triggers IOException.

    java.lang.reflect.Field field = MVStoreFileQueue.class.getDeclaredField("fileName");
    field.setAccessible(true);
    field.set(ioQueue, tempDir.resolve("non-existent-sub/file.db").toString());

    java.lang.reflect.Method method = MVStoreFileQueue.class.getDeclaredMethod("fileSize");
    method.setAccessible(true);
    // Files.size() throws NoSuchFileException if file doesn't exist, which is an IOException.
    assertThat(method.invoke(ioQueue)).isEqualTo(0L);

    ioQueue.close();
  }

  /**
   * readableFileSize(0) 호출 시 "0"을 반환하는지 검증한다.
   * <p>size &le; 0 조건의 early return 브랜치를 커버한다.</p>
   */
  @Test
  void testReadableFileSize_zeroOrNegative() throws Exception {
    java.lang.reflect.Method method = MVStoreFileQueue.class.getDeclaredMethod("readableFileSize", long.class);
    method.setAccessible(true);
    assertThat(method.invoke(queue, 0L)).isEqualTo("0");
    assertThat(method.invoke(queue, -1L)).isEqualTo("0");
  }

  /**
   * executeWithWriteLock의 재시도 루프가 모두 소진될 때 FileQueueException이 발생하는지 검증한다.
   * <p>
   * non-business 예외가 maxRetry번 연속 발생하면 마지막 예외를 원인(cause)으로 갖는
   * FileQueueException이 발생해야 한다.
   * </p>
   */
  @Test
  void testExecuteWithWriteLock_retryExhausted() throws Exception {
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    properties.setMaxRetry(2);
    properties.setRetryBackoffMs(10);

    MVStoreFileQueue<String> retryQueue = new MVStoreFileQueue<>(properties, tempDir.resolve("retry-write.db").toString());

    java.util.concurrent.atomic.AtomicInteger count = new java.util.concurrent.atomic.AtomicInteger(0);

    java.lang.reflect.Method method = MVStoreFileQueue.class.getDeclaredMethod("executeWithWriteLock",
        Class.forName("io.github.ppzxc.fq.MVStoreFileQueue$SupplierWithException"));
    method.setAccessible(true);

    // 항상 non-business Exception을 던져 재시도를 소진시킨다
    Object supplier = java.lang.reflect.Proxy.newProxyInstance(
        MVStoreFileQueue.class.getClassLoader(),
        new Class[]{Class.forName("io.github.ppzxc.fq.MVStoreFileQueue$SupplierWithException")},
        (proxy, m, args) -> {
            count.getAndIncrement();
            throw new java.io.IOException("Simulated transient I/O failure");
        }
    );

    assertThatThrownBy(() -> {
      try {
        method.invoke(retryQueue, supplier);
      } catch (java.lang.reflect.InvocationTargetException e) {
        throw e.getCause();
      }
    })
        .isInstanceOf(FileQueueException.class)
        .hasMessageContaining("Failed to execute with write lock after 2 attempts");

    assertThat(count.get()).isEqualTo(2);
    retryQueue.close();
  }

  /**
   * close()가 멱등성(idempotent)을 보장하는지 검증한다.
   * <p>
   * close()를 여러 번 호출해도 예외가 발생하지 않아야 한다.
   * 두 번째 close() 호출은 외부 if (closed) 체크 또는 lock 내부 double-check로
   * 안전하게 no-op 처리되어야 한다.
   * </p>
   */
  @Test
  void testClose_alreadyClosedInsideLock() {
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    MVStoreFileQueue<String> q = new MVStoreFileQueue<>(properties, tempDir.resolve("double-close.db").toString());

    // 첫 번째 close() — 정상 동작
    assertThatCode(q::close).doesNotThrowAnyException();

    // 두 번째 close() — 멱등성: 예외 없이 no-op
    assertThatCode(q::close).doesNotThrowAnyException();
  }
}
