package io.github.ppzxc.fq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SafeObjectInputStreamTest {

  @TempDir
  Path tempDir;

  private byte[] serialize(Object obj) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(obj);
    oos.close();
    return baos.toByteArray();
  }

  @DisplayName("허용 목록에 있는 클래스는 역직렬화 성공")
  @Test
  void allowedClass_deserializesSuccessfully() throws Exception {
    // given
    Set<String> allowed = new HashSet<>();
    allowed.add("java.lang.String");
    byte[] bytes = serialize("hello");

    // when
    SafeObjectInputStream ois = new SafeObjectInputStream(new ByteArrayInputStream(bytes), allowed);
    Object result = ois.readObject();

    // then
    assertThat(result).isEqualTo("hello");
  }

  @DisplayName("허용 목록에 없는 클래스는 역직렬화 거부")
  @Test
  void blockedClass_throwsException() throws Exception {
    // given
    Set<String> allowed = new HashSet<>();
    allowed.add("java.lang.String"); // only String allowed
    byte[] bytes = serialize(Integer.valueOf(42)); // Integer not in whitelist

    // when, then
    SafeObjectInputStream ois = new SafeObjectInputStream(new ByteArrayInputStream(bytes), allowed);
    assertThatThrownBy(() -> ois.readObject())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("java.lang.Integer");
  }

  @DisplayName("허용 목록이 비어 있으면 모든 클래스 허용 (하위 호환)")
  @Test
  void emptyAllowedClasses_allowsAll() throws Exception {
    // given — empty set means allow all
    Set<String> allowed = Collections.emptySet();
    byte[] bytes = serialize(Integer.valueOf(99));

    // when
    SafeObjectInputStream ois = new SafeObjectInputStream(new ByteArrayInputStream(bytes), allowed);
    Object result = ois.readObject();

    // then
    assertThat(result).isEqualTo(99);
  }

  @DisplayName("FQDataType으로 String 저장 후 조회 성공")
  @Test
  void fqDataType_writeAndRead_string() {
    // given
    MVStoreFileQueueProperties props = new MVStoreFileQueueProperties();
    Set<String> allowed = new HashSet<>();
    allowed.add("java.lang.String");
    props.setAllowedClasses(allowed);

    String path = tempDir.resolve("queue.db").toFile().getAbsolutePath();
    FileQueue<String> queue = FileQueueFactory.createMVStoreFileQueue(path, props);

    // when
    queue.enqueue("safe-value");
    String result = queue.dequeue();
    queue.close();

    // then
    assertThat(result).isEqualTo("safe-value");
  }

  @DisplayName("FQDataType 기본 설정 (허용 목록 비어있음)에서 String 저장 후 조회 성공")
  @Test
  void fqDataType_defaultProps_allowsAll() {
    // given — default props with empty allowedClasses allows all
    MVStoreFileQueueProperties props = new MVStoreFileQueueProperties();
    String path = tempDir.resolve("queue2.db").toFile().getAbsolutePath();
    FileQueue<String> queue = FileQueueFactory.createMVStoreFileQueue(path, props);

    // when
    queue.enqueue("backward-compat");
    String result = queue.dequeue();
    queue.close();

    // then
    assertThat(result).isEqualTo("backward-compat");
  }

  @DisplayName("허용 목록에 없는 String 배열 역직렬화 거부 ([L...;  클래스 이름으로 resolveClass 호출)")
  @Test
  void allowedClass_objectArrayNotInWhitelist_rejected() throws Exception {
    // given — String not in whitelist, String[] should be blocked via [Ljava.lang.String;
    // Note: String itself uses TC_STRING and does NOT trigger resolveClass,
    //       but String[] array class descriptor DOES trigger resolveClass with "[Ljava.lang.String;"
    Set<String> allowed = new HashSet<>();
    allowed.add("java.lang.Integer"); // only Integer, not String
    byte[] bytes = serialize(new String[]{"hello"}); // String[] triggers resolveClass

    // when, then
    SafeObjectInputStream ois = new SafeObjectInputStream(new ByteArrayInputStream(bytes), allowed);
    assertThatThrownBy(() -> ois.readObject())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("[Ljava.lang.String;");
  }

  @DisplayName("허용 목록에 컴포넌트 타입이 있으면 배열 타입 허용")
  @Test
  void allowedClass_arrayOfAllowedComponent_allowed() throws Exception {
    // given
    Set<String> allowed = new HashSet<>();
    allowed.add("java.lang.String");
    String[] arr = {"a", "b"};
    byte[] bytes = serialize(arr);

    // when
    SafeObjectInputStream ois = new SafeObjectInputStream(new ByteArrayInputStream(bytes), allowed);
    Object result = ois.readObject();

    // then
    assertThat(result).isInstanceOf(String[].class);
  }
}
