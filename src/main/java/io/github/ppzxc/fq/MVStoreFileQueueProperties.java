package io.github.ppzxc.fq;

import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class MVStoreFileQueueProperties {

  private String queueName = "queue";
  private int batchSize = 1000;
  private long maxSize = Long.MAX_VALUE;
  private int autoCompactFillRate = 90; // avoid bug for integrity, consistency
  private boolean useCompress = true;
  private boolean autoCommitDisabled = false;
  private int autoCommitBufferSize = 1024;
  private int maxRetry = 3;
  private long retryDelay = 100;
  private int cacheSize = 1; // 1MB
  private boolean fair = true;
  private int tryLockTimeout = 1;
  private TimeUnit tryLockTimeunit = TimeUnit.SECONDS;
  private int retryBackoffMs = 100;
  private int maxCompactTime = 60 * 1000;
  private long compactByFileSize = 50 * 1024 * 1024; // 50 MB

  public void setQueueName(String queueName) {
    if (queueName == null || queueName.trim().isEmpty()) {
      throw new IllegalArgumentException("queueName cannot be null or empty");
    }
    this.queueName = queueName;
  }

  public void setBatchSize(int batchSize) {
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be positive");
    }
    this.batchSize = batchSize;
  }

  public void setMaxSize(long maxSize) {
    if (maxSize < 0) {
      throw new IllegalArgumentException("maxSize cannot be negative");
    }
    this.maxSize = maxSize;
  }

  public void setAutoCompactFillRate(int autoCompactFillRate) {
    if (autoCompactFillRate < 0 || autoCompactFillRate > 100) {
      throw new IllegalArgumentException("autoCompactFillRate must be between 0 and 100");
    }
    this.autoCompactFillRate = autoCompactFillRate;
  }

  public void setUseCompress(boolean useCompress) {
    this.useCompress = useCompress;
  }

  public void setAutoCommitDisabled(boolean autoCommitDisabled) {
    this.autoCommitDisabled = autoCommitDisabled;
  }

  public void setAutoCommitBufferSize(int autoCommitBufferSize) {
    if (autoCommitBufferSize <= 0) {
      throw new IllegalArgumentException("autoCommitBufferSize must be positive");
    }
    this.autoCommitBufferSize = autoCommitBufferSize;
  }

  public void setMaxRetry(int maxRetry) {
    if (maxRetry < 0) {
      throw new IllegalArgumentException("maxRetry cannot be negative");
    }
    this.maxRetry = maxRetry;
  }

  public void setRetryDelay(long retryDelay) {
    if (retryDelay < 0) {
      throw new IllegalArgumentException("retryDelay cannot be negative");
    }
    this.retryDelay = retryDelay;
  }

  public void setCacheSize(int cacheSize) {
    if (cacheSize <= 0) {
      throw new IllegalArgumentException("cacheSize must be positive");
    }
    this.cacheSize = cacheSize;
  }

  public void setFair(boolean fair) {
    this.fair = fair;
  }

  public void setTryLockTimeout(int tryLockTimeout) {
    if (tryLockTimeout < 0) {
      throw new IllegalArgumentException("tryLockTimeout cannot be negative");
    }
    this.tryLockTimeout = tryLockTimeout;
  }

  public void setTryLockTimeunit(TimeUnit tryLockTimeunit) {
    if (tryLockTimeunit == null) {
      throw new IllegalArgumentException("tryLockTimeunit cannot be null");
    }
    this.tryLockTimeunit = tryLockTimeunit;
  }

  public void setRetryBackoffMs(int retryBackoffMs) {
    if (retryBackoffMs < 0) {
      throw new IllegalArgumentException("retryBackoffMs cannot be negative");
    }
    this.retryBackoffMs = retryBackoffMs;
  }

  public void setMaxCompactTime(int maxCompactTime) {
    if (maxCompactTime <= 0) {
      throw new IllegalArgumentException("maxCompactTime must be positive");
    }
    this.maxCompactTime = maxCompactTime;
  }

  public void setCompactByFileSize(long compactByFileSize) {
    if (compactByFileSize <= 0) {
      throw new IllegalArgumentException("compactByFileSize must be positive");
    }
    this.compactByFileSize = compactByFileSize;
  }

}
