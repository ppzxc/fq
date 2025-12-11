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

}
