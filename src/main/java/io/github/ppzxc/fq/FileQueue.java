package io.github.ppzxc.fq;

import java.util.List;

/**
 * A persistent file-based FIFO queue that stores serializable objects.
 *
 * <p>Implementations are expected to be thread-safe and to persist data across
 * process restarts using an underlying file storage engine.</p>
 *
 * @param <T> the type of elements held in this queue; must be serializable
 */
public interface FileQueue<T> {

  /**
   * Returns the absolute file path used for persistent storage.
   *
   * @return the file name (absolute path) of the underlying storage file
   */
  String fileName();

  /**
   * Adds a single element to the tail of this queue.
   *
   * @param value the element to add; must not be {@code null}
   * @return {@code true} if the element was successfully added
   * @throws IllegalArgumentException if {@code value} is {@code null}
   * @throws FileQueueException       if the queue is full (maxSize exceeded)
   * @throws IllegalStateException    if the queue has been closed
   */
  boolean enqueue(T value);

  /**
   * Adds all elements in the given list to the tail of this queue atomically.
   *
   * <p>All elements are validated before any are added. If {@code currentSize + list.size > maxSize},
   * a {@link FileQueueException} is thrown and no elements are added.
   * Unlike single-element enqueue, a batch can fill the queue exactly to maxSize.</p>
   *
   * @param value the list of elements to add; must not be {@code null} and must
   *              not contain {@code null} elements
   * @return {@code true} if all elements were successfully added, or {@code true}
   *         if the list is empty
   * @throws IllegalArgumentException if {@code value} is {@code null} or contains
   *                                  a {@code null} element
   * @throws FileQueueException       if adding all elements would exceed maxSize
   * @throws IllegalStateException    if the queue has been closed
   */
  boolean enqueue(List<T> value);

  /**
   * Removes and returns the element at the head of this queue.
   *
   * @return the head element, or {@code null} if the queue is empty
   * @throws IllegalStateException if the queue has been closed
   */
  T dequeue();

  /**
   * Removes and returns up to {@code size} elements from the head of this queue.
   *
   * <p>If fewer than {@code size} elements are available, all available elements
   * are returned.</p>
   *
   * @param size the maximum number of elements to dequeue; must be &ge; 0
   * @return a list of dequeued elements (may be empty, never {@code null})
   * @throws IllegalArgumentException if {@code size} is negative
   * @throws IllegalStateException    if the queue has been closed
   */
  List<T> dequeue(int size);

  /**
   * Returns {@code true} if this queue contains no elements.
   *
   * @return {@code true} if the queue is empty
   * @throws IllegalStateException if the queue has been closed
   */
  boolean isEmpty();

  /**
   * Returns the number of elements currently in this queue.
   *
   * @return the current queue size (tail - head)
   * @throws IllegalStateException if the queue has been closed
   */
  long size();

  /**
   * Logs runtime metrics for this queue instance under the given name.
   *
   * <p>Logged fields include total operations, total commits, current size,
   * head pointer, and tail pointer.</p>
   *
   * @param name a label used to identify this queue in the log output
   */
  void metric(String name);

  /**
   * Flushes pending data and closes the underlying storage.
   *
   * <p>This method is idempotent; calling it multiple times has no additional effect.
   * After closing, all mutating operations will throw {@link IllegalStateException}.</p>
   */
  void close();

  /**
   * Compacts the underlying storage file if its size exceeds the configured threshold.
   *
   * <p>Compaction reclaims disk space by rewriting the storage file, removing
   * obsolete data. If the file size is below the configured
   * {@code compactByFileSize} threshold, this is a no-op (logged only).</p>
   *
   * @throws IllegalStateException if the queue has been closed
   */
  void compactFile();
}
