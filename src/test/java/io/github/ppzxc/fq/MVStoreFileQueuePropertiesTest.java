package io.github.ppzxc.fq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MVStoreFileQueuePropertiesTest {

  @DisplayName("default values are set correctly")
  @Test
  void defaultValues() {
    // when
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // then
    assertThat(properties.getQueueName()).isEqualTo("queue");
    assertThat(properties.getBatchSize()).isEqualTo(1000);
    assertThat(properties.getMaxSize()).isEqualTo(Long.MAX_VALUE);
    assertThat(properties.getAutoCompactFillRate()).isEqualTo(90);
    assertThat(properties.isUseCompress()).isTrue();
    assertThat(properties.isAutoCommitDisabled()).isFalse();
    assertThat(properties.getAutoCommitBufferSize()).isEqualTo(1024);
    assertThat(properties.getMaxRetry()).isEqualTo(3);
    assertThat(properties.getRetryDelay()).isEqualTo(100);
    assertThat(properties.getCacheSize()).isEqualTo(1);
    assertThat(properties.isFair()).isTrue();
    assertThat(properties.getTryLockTimeout()).isEqualTo(1);
    assertThat(properties.getTryLockTimeunit()).isEqualTo(TimeUnit.SECONDS);
    assertThat(properties.getRetryBackoffMs()).isEqualTo(100);
    assertThat(properties.getMaxCompactTime()).isEqualTo(60 * 1000);
    assertThat(properties.getCompactByFileSize()).isEqualTo(50 * 1024 * 1024);
  }

  @DisplayName("setQueueName - valid value")
  @Test
  void setQueueName_valid() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when
    properties.setQueueName("myQueue");

    // then
    assertThat(properties.getQueueName()).isEqualTo("myQueue");
  }

  @DisplayName("setQueueName - null throws exception")
  @Test
  void setQueueName_null() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when, then
    assertThatThrownBy(() -> properties.setQueueName(null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("queueName cannot be null or empty");
  }

  @DisplayName("setQueueName - empty throws exception")
  @Test
  void setQueueName_empty() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when, then
    assertThatThrownBy(() -> properties.setQueueName(""))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("queueName cannot be null or empty");
  }

  @DisplayName("setQueueName - whitespace throws exception")
  @Test
  void setQueueName_whitespace() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when, then
    assertThatThrownBy(() -> properties.setQueueName("   "))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("queueName cannot be null or empty");
  }

  @DisplayName("setBatchSize - valid value")
  @Test
  void setBatchSize_valid() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when
    properties.setBatchSize(500);

    // then
    assertThat(properties.getBatchSize()).isEqualTo(500);
  }

  @DisplayName("setBatchSize - zero throws exception")
  @Test
  void setBatchSize_zero() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when, then
    assertThatThrownBy(() -> properties.setBatchSize(0))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("batchSize must be positive");
  }

  @DisplayName("setBatchSize - negative throws exception")
  @Test
  void setBatchSize_negative() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when, then
    assertThatThrownBy(() -> properties.setBatchSize(-1))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("batchSize must be positive");
  }

  @DisplayName("setMaxSize - valid value")
  @Test
  void setMaxSize_valid() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when
    properties.setMaxSize(1000);

    // then
    assertThat(properties.getMaxSize()).isEqualTo(1000);
  }

  @DisplayName("setMaxSize - zero is valid")
  @Test
  void setMaxSize_zero() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when
    properties.setMaxSize(0);

    // then
    assertThat(properties.getMaxSize()).isEqualTo(0);
  }

  @DisplayName("setMaxSize - negative throws exception")
  @Test
  void setMaxSize_negative() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when, then
    assertThatThrownBy(() -> properties.setMaxSize(-1))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("maxSize cannot be negative");
  }

  @DisplayName("setAutoCompactFillRate - valid value")
  @Test
  void setAutoCompactFillRate_valid() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when
    properties.setAutoCompactFillRate(50);

    // then
    assertThat(properties.getAutoCompactFillRate()).isEqualTo(50);
  }

  @DisplayName("setAutoCompactFillRate - zero is valid")
  @Test
  void setAutoCompactFillRate_zero() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when
    properties.setAutoCompactFillRate(0);

    // then
    assertThat(properties.getAutoCompactFillRate()).isEqualTo(0);
  }

  @DisplayName("setAutoCompactFillRate - 100 is valid")
  @Test
  void setAutoCompactFillRate_hundred() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when
    properties.setAutoCompactFillRate(100);

    // then
    assertThat(properties.getAutoCompactFillRate()).isEqualTo(100);
  }

  @DisplayName("setAutoCompactFillRate - negative throws exception")
  @Test
  void setAutoCompactFillRate_negative() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when, then
    assertThatThrownBy(() -> properties.setAutoCompactFillRate(-1))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("autoCompactFillRate must be between 0 and 100");
  }

  @DisplayName("setAutoCompactFillRate - over 100 throws exception")
  @Test
  void setAutoCompactFillRate_over100() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when, then
    assertThatThrownBy(() -> properties.setAutoCompactFillRate(101))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("autoCompactFillRate must be between 0 and 100");
  }

  @DisplayName("setUseCompress - true")
  @Test
  void setUseCompress_true() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when
    properties.setUseCompress(true);

    // then
    assertThat(properties.isUseCompress()).isTrue();
  }

  @DisplayName("setUseCompress - false")
  @Test
  void setUseCompress_false() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when
    properties.setUseCompress(false);

    // then
    assertThat(properties.isUseCompress()).isFalse();
  }

  @DisplayName("setAutoCommitDisabled - true")
  @Test
  void setAutoCommitDisabled_true() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when
    properties.setAutoCommitDisabled(true);

    // then
    assertThat(properties.isAutoCommitDisabled()).isTrue();
  }

  @DisplayName("setAutoCommitDisabled - false")
  @Test
  void setAutoCommitDisabled_false() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when
    properties.setAutoCommitDisabled(false);

    // then
    assertThat(properties.isAutoCommitDisabled()).isFalse();
  }

  @DisplayName("setAutoCommitBufferSize - valid value")
  @Test
  void setAutoCommitBufferSize_valid() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when
    properties.setAutoCommitBufferSize(2048);

    // then
    assertThat(properties.getAutoCommitBufferSize()).isEqualTo(2048);
  }

  @DisplayName("setAutoCommitBufferSize - zero throws exception")
  @Test
  void setAutoCommitBufferSize_zero() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when, then
    assertThatThrownBy(() -> properties.setAutoCommitBufferSize(0))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("autoCommitBufferSize must be positive");
  }

  @DisplayName("setAutoCommitBufferSize - negative throws exception")
  @Test
  void setAutoCommitBufferSize_negative() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when, then
    assertThatThrownBy(() -> properties.setAutoCommitBufferSize(-1))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("autoCommitBufferSize must be positive");
  }

  @DisplayName("setMaxRetry - valid value")
  @Test
  void setMaxRetry_valid() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when
    properties.setMaxRetry(5);

    // then
    assertThat(properties.getMaxRetry()).isEqualTo(5);
  }

  @DisplayName("setMaxRetry - zero is valid")
  @Test
  void setMaxRetry_zero() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when
    properties.setMaxRetry(0);

    // then
    assertThat(properties.getMaxRetry()).isEqualTo(0);
  }

  @DisplayName("setMaxRetry - negative throws exception")
  @Test
  void setMaxRetry_negative() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when, then
    assertThatThrownBy(() -> properties.setMaxRetry(-1))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("maxRetry cannot be negative");
  }

  @DisplayName("setRetryDelay - valid value")
  @Test
  void setRetryDelay_valid() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when
    properties.setRetryDelay(200);

    // then
    assertThat(properties.getRetryDelay()).isEqualTo(200);
  }

  @DisplayName("setRetryDelay - zero is valid")
  @Test
  void setRetryDelay_zero() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when
    properties.setRetryDelay(0);

    // then
    assertThat(properties.getRetryDelay()).isEqualTo(0);
  }

  @DisplayName("setRetryDelay - negative throws exception")
  @Test
  void setRetryDelay_negative() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when, then
    assertThatThrownBy(() -> properties.setRetryDelay(-1))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("retryDelay cannot be negative");
  }

  @DisplayName("setCacheSize - valid value")
  @Test
  void setCacheSize_valid() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when
    properties.setCacheSize(10);

    // then
    assertThat(properties.getCacheSize()).isEqualTo(10);
  }

  @DisplayName("setCacheSize - zero throws exception")
  @Test
  void setCacheSize_zero() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when, then
    assertThatThrownBy(() -> properties.setCacheSize(0))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("cacheSize must be positive");
  }

  @DisplayName("setCacheSize - negative throws exception")
  @Test
  void setCacheSize_negative() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when, then
    assertThatThrownBy(() -> properties.setCacheSize(-1))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("cacheSize must be positive");
  }

  @DisplayName("setFair - true")
  @Test
  void setFair_true() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when
    properties.setFair(true);

    // then
    assertThat(properties.isFair()).isTrue();
  }

  @DisplayName("setFair - false")
  @Test
  void setFair_false() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when
    properties.setFair(false);

    // then
    assertThat(properties.isFair()).isFalse();
  }

  @DisplayName("setTryLockTimeout - valid value")
  @Test
  void setTryLockTimeout_valid() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when
    properties.setTryLockTimeout(5);

    // then
    assertThat(properties.getTryLockTimeout()).isEqualTo(5);
  }

  @DisplayName("setTryLockTimeout - zero is valid")
  @Test
  void setTryLockTimeout_zero() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when
    properties.setTryLockTimeout(0);

    // then
    assertThat(properties.getTryLockTimeout()).isEqualTo(0);
  }

  @DisplayName("setTryLockTimeout - negative throws exception")
  @Test
  void setTryLockTimeout_negative() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when, then
    assertThatThrownBy(() -> properties.setTryLockTimeout(-1))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("tryLockTimeout cannot be negative");
  }

  @DisplayName("setTryLockTimeunit - valid value")
  @Test
  void setTryLockTimeunit_valid() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when
    properties.setTryLockTimeunit(TimeUnit.MILLISECONDS);

    // then
    assertThat(properties.getTryLockTimeunit()).isEqualTo(TimeUnit.MILLISECONDS);
  }

  @DisplayName("setTryLockTimeunit - null throws exception")
  @Test
  void setTryLockTimeunit_null() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when, then
    assertThatThrownBy(() -> properties.setTryLockTimeunit(null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("tryLockTimeunit cannot be null");
  }

  @DisplayName("setRetryBackoffMs - valid value")
  @Test
  void setRetryBackoffMs_valid() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when
    properties.setRetryBackoffMs(200);

    // then
    assertThat(properties.getRetryBackoffMs()).isEqualTo(200);
  }

  @DisplayName("setRetryBackoffMs - zero is valid")
  @Test
  void setRetryBackoffMs_zero() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when
    properties.setRetryBackoffMs(0);

    // then
    assertThat(properties.getRetryBackoffMs()).isEqualTo(0);
  }

  @DisplayName("setRetryBackoffMs - negative throws exception")
  @Test
  void setRetryBackoffMs_negative() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when, then
    assertThatThrownBy(() -> properties.setRetryBackoffMs(-1))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("retryBackoffMs cannot be negative");
  }

  @DisplayName("setMaxCompactTime - valid value")
  @Test
  void setMaxCompactTime_valid() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when
    properties.setMaxCompactTime(120000);

    // then
    assertThat(properties.getMaxCompactTime()).isEqualTo(120000);
  }

  @DisplayName("setMaxCompactTime - zero throws exception")
  @Test
  void setMaxCompactTime_zero() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when, then
    assertThatThrownBy(() -> properties.setMaxCompactTime(0))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("maxCompactTime must be positive");
  }

  @DisplayName("setMaxCompactTime - negative throws exception")
  @Test
  void setMaxCompactTime_negative() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when, then
    assertThatThrownBy(() -> properties.setMaxCompactTime(-1))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("maxCompactTime must be positive");
  }

  @DisplayName("setCompactByFileSize - valid value")
  @Test
  void setCompactByFileSize_valid() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when
    properties.setCompactByFileSize(100 * 1024 * 1024);

    // then
    assertThat(properties.getCompactByFileSize()).isEqualTo(100 * 1024 * 1024);
  }

  @DisplayName("setCompactByFileSize - zero throws exception")
  @Test
  void setCompactByFileSize_zero() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when, then
    assertThatThrownBy(() -> properties.setCompactByFileSize(0))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("compactByFileSize must be positive");
  }

  @DisplayName("setCompactByFileSize - negative throws exception")
  @Test
  void setCompactByFileSize_negative() {
    // given
    MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();

    // when, then
    assertThatThrownBy(() -> properties.setCompactByFileSize(-1))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("compactByFileSize must be positive");
  }
}
