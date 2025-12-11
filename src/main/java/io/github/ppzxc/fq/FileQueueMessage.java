package io.github.ppzxc.fq;

import lombok.Builder;
import lombok.Getter;
import lombok.With;

@With
@Getter
@Builder
public class FileQueueMessage {

  private final Class<?> clazz;
  private final Object message;
}
