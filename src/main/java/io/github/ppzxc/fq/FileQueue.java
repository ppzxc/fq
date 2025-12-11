package io.github.ppzxc.fq;

import java.util.List;

public interface FileQueue<T> {

  String fileName();

  boolean enqueue(T value);

  boolean enqueue(List<T> value);

  T dequeue();

  List<T> dequeue(int size);

  boolean isEmpty();

  long size();

  void metric(String name);

  void close();

  void compactFile();
}
