package io.github.ppzxc.fq;

import java.io.Serializable;
import java.nio.file.FileSystems;

@SuppressWarnings("unused")
public final class FileQueueFactory {

  private FileQueueFactory() {
  }

  public static <T extends Serializable> FileQueue<T> createMVStoreFileQueue(
    MVStoreFileQueueProperties mvStoreFileQueueProperties, String fileName) {
    if (fileName == null || fileName.trim().isEmpty()) {
      throw new IllegalArgumentException("[FileQueueFactory] fileName cannot be null or empty");
    }
    return new MVStoreFileQueue<>(mvStoreFileQueueProperties,
      String.join(FileSystems.getDefault().getSeparator(), System.getProperty("user.dir"), "sys", "que", fileName));
  }

  public static <T extends Serializable> FileQueue<T> createMVStoreFileQueue(String path,
    MVStoreFileQueueProperties mvStoreFileQueueProperties) {
    if (path == null || path.trim().isEmpty()) {
      throw new IllegalArgumentException("[FileQueueFactory] path cannot be null or empty");
    }
    return new MVStoreFileQueue<>(mvStoreFileQueueProperties, path);
  }

  public static <T extends Serializable> FileQueue<T> createMVStoreFileQueue() {
    return createMVStoreFileQueue(new MVStoreFileQueueProperties(), "local");
  }
}
