package io.github.ppzxc.fq;

import lombok.Builder;
import lombok.With;

@With
@Builder
public class FileQueueMessage {

  private final Class<?> clazz;
  private final Object message;
}
