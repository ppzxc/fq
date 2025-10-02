package io.github.ppzxc.fq;

import java.util.concurrent.TimeUnit;

public class MVStoreFileQueueProperties {

  private String fileName;
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

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public String getQueueName() {
    return queueName;
  }

  public void setQueueName(String queueName) {
    this.queueName = queueName;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public long getMaxSize() {
    return maxSize;
  }

  public void setMaxSize(long maxSize) {
    this.maxSize = maxSize;
  }

  public int getAutoCompactFillRate() {
    return autoCompactFillRate;
  }

  public void setAutoCompactFillRate(int autoCompactFillRate) {
    this.autoCompactFillRate = autoCompactFillRate;
  }

  public boolean isUseCompress() {
    return useCompress;
  }

  public void setUseCompress(boolean useCompress) {
    this.useCompress = useCompress;
  }

  public boolean isAutoCommitDisabled() {
    return autoCommitDisabled;
  }

  public void setAutoCommitDisabled(boolean autoCommitDisabled) {
    this.autoCommitDisabled = autoCommitDisabled;
  }

  public int getAutoCommitBufferSize() {
    return autoCommitBufferSize;
  }

  public void setAutoCommitBufferSize(int autoCommitBufferSize) {
    this.autoCommitBufferSize = autoCommitBufferSize;
  }

  public int getMaxRetry() {
    return maxRetry;
  }

  public void setMaxRetry(int maxRetry) {
    this.maxRetry = maxRetry;
  }

  public long getRetryDelay() {
    return retryDelay;
  }

  public void setRetryDelay(long retryDelay) {
    this.retryDelay = retryDelay;
  }

  public int getCacheSize() {
    return cacheSize;
  }

  public void setCacheSize(int cacheSize) {
    this.cacheSize = cacheSize;
  }

  public boolean isFair() {
    return fair;
  }

  public void setFair(boolean fair) {
    this.fair = fair;
  }

  public int getTryLockTimeout() {
    return tryLockTimeout;
  }

  public void setTryLockTimeout(int tryLockTimeout) {
    this.tryLockTimeout = tryLockTimeout;
  }

  public TimeUnit getTryLockTimeunit() {
    return tryLockTimeunit;
  }

  public void setTryLockTimeunit(TimeUnit tryLockTimeunit) {
    this.tryLockTimeunit = tryLockTimeunit;
  }
}
