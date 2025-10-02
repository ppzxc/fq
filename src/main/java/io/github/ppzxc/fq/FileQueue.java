package io.github.ppzxc.fq;

import java.util.Optional;

public interface FileQueue<T> {

  void enqueue(T value);

  Optional<T> dequeue();

  boolean isEmpty();

  long size();

  void metric(String name);

  void close();
}
