package io.github.ppzxc.fq;

import java.io.Serializable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@RequiredArgsConstructor
public class FileQueueMessage implements Serializable {

  private static final long serialVersionUID = 1255425631068333713L;
  private Class<?> clazz;
  private Object message;

  public static FileQueueMessageBuilder builder() {
    return new FileQueueMessageBuilder();
  }

  public static final class FileQueueMessageBuilder {

    private Class<?> clazz;
    private Object message;

    private FileQueueMessageBuilder() {
    }

    public FileQueueMessageBuilder clazz(Class<?> clazz) {
      this.clazz = clazz;
      return this;
    }

    public FileQueueMessageBuilder message(Object message) {
      this.message = message;
      return this;
    }

    public FileQueueMessage build() {
      FileQueueMessage fileQueueMessage = new FileQueueMessage();
      fileQueueMessage.setClazz(clazz);
      fileQueueMessage.setMessage(message);
      return fileQueueMessage;
    }
  }
}
