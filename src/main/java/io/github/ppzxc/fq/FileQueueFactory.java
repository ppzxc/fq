package io.github.ppzxc.fq;

import java.io.Serializable;

@SuppressWarnings("unused")
public final class FileQueueFactory {

  private FileQueueFactory() {
  }

  public static <T extends Serializable> FileQueue<T> createMVStoreFileQueue(
    MVStoreFileQueueProperties mvStoreFileQueueProperties) {
    return new MVStoreFileQueue<>(mvStoreFileQueueProperties);
  }

  public static <T extends Serializable> FileQueue<T> createMVStoreFileQueue() {
    return createMVStoreFileQueue(new MVStoreFileQueueProperties());
  }
}
