package io.github.ppzxc.fq;

public class FileQueueException extends RuntimeException {

  private static final long serialVersionUID = 7590349757277733909L;

  public FileQueueException(String message) {
    super(message);
  }

  public FileQueueException(String message, Throwable cause) {
    super(message, cause);
  }
}
