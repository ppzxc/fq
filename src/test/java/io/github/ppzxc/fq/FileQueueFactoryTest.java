package io.github.ppzxc.fq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileQueueFactoryTest {

  public static final String NAME = "test_queue.db";
  @TempDir
  Path tempDir;

  @AfterEach
  void tearDown() throws IOException {
    Files.deleteIfExists(Paths.get(String.join(FileSystems.getDefault().getSeparator(), System.getProperty("user.dir"), "sys", "que", NAME)));
  }

  @DisplayName("createMVStoreFileQueue with fileName - success")
  @Test
  void createWithFileName_success() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when
    FileQueue<String> queue = FileQueueFactory.createMVStoreFileQueue(properties, NAME);

    // then
    assertThat(queue).isNotNull();
    assertThat(queue.fileName()).contains(NAME);
    queue.close();
  }

  @DisplayName("createMVStoreFileQueue with fileName - null fileName throws exception")
  @Test
  void createWithFileName_nullFileName() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when, then
    assertThatThrownBy(() -> FileQueueFactory.createMVStoreFileQueue(properties, null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("[FileQueueFactory] fileName cannot be null or empty");
  }

  @DisplayName("createMVStoreFileQueue with fileName - empty fileName throws exception")
  @Test
  void createWithFileName_emptyFileName() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when, then
    assertThatThrownBy(() -> FileQueueFactory.createMVStoreFileQueue(properties, ""))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("[FileQueueFactory] fileName cannot be null or empty");
  }

  @DisplayName("createMVStoreFileQueue with fileName - whitespace fileName throws exception")
  @Test
  void createWithFileName_whitespaceFileName() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when, then
    assertThatThrownBy(() -> FileQueueFactory.createMVStoreFileQueue(properties, "   "))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("[FileQueueFactory] fileName cannot be null or empty");
  }

  @DisplayName("createMVStoreFileQueue with path - success")
  @Test
  void createWithPath_success() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
    String path = tempDir.resolve(NAME).toFile().getAbsolutePath();

    // when
    FileQueue<String> queue = FileQueueFactory.createMVStoreFileQueue(path, properties);

    // then
    assertThat(queue).isNotNull();
    assertThat(queue.fileName()).isEqualTo(path);
    queue.close();
  }

  @DisplayName("createMVStoreFileQueue with path - null path throws exception")
  @Test
  void createWithPath_nullPath() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when, then
    assertThatThrownBy(() -> FileQueueFactory.createMVStoreFileQueue(null, properties))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("[FileQueueFactory] path cannot be null or empty");
  }

  @DisplayName("createMVStoreFileQueue with path - empty path throws exception")
  @Test
  void createWithPath_emptyPath() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when, then
    assertThatThrownBy(() -> FileQueueFactory.createMVStoreFileQueue("", properties))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("[FileQueueFactory] path cannot be null or empty");
  }

  @DisplayName("createMVStoreFileQueue with path - whitespace path throws exception")
  @Test
  void createWithPath_whitespacePath() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when, then
    assertThatThrownBy(() -> FileQueueFactory.createMVStoreFileQueue("   ", properties))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("[FileQueueFactory] path cannot be null or empty");
  }

  @DisplayName("createMVStoreFileQueue with default - success")
  @Test
  void createDefault_success() throws IOException {
    try {
      // when
      FileQueue<String> queue = FileQueueFactory.createMVStoreFileQueue();

      // then
      assertThat(queue).isNotNull();
      queue.close();
    } finally {
      Files.deleteIfExists(Paths.get(String.join(FileSystems.getDefault().getSeparator(), System.getProperty("user.dir"), "sys", "que", "local")));
    }
  }
}
