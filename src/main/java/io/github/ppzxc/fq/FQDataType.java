package io.github.ppzxc.fq;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Set;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.BasicDataType;


/**
 * H2 MVStore {@link org.h2.mvstore.type.DataType} implementation that uses standard Java
 * serialization with an optional class whitelist for safe deserialization (CWE-502).
 *
 * <p>Serialization format: 4-byte int (length) followed by raw Java serialization bytes.</p>
 *
 * <p>Note: This format is intentionally different from H2's built-in {@code ObjectDataType}.
 * Existing queues written with the default ObjectDataType must be recreated when upgrading
 * to this DataType.</p>
 */
class FQDataType<T extends Serializable> extends BasicDataType<T> {

  private final Set<String> allowedClasses;

  FQDataType(Set<String> allowedClasses) {
    this.allowedClasses = allowedClasses;
  }

  @Override
  public int getMemory(T obj) {
    return 1024;
  }

  @Override
  public void write(WriteBuffer buf, T obj) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(obj);
      oos.close();
      byte[] bytes = baos.toByteArray();
      buf.putInt(bytes.length);
      buf.put(bytes);
    } catch (IOException e) {
      throw new RuntimeException("[FQDataType] Failed to serialize object: " + e.getMessage(), e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public T read(ByteBuffer buf) {
    int length = buf.getInt();
    byte[] bytes = new byte[length];
    buf.get(bytes);
    try {
      ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
      SafeObjectInputStream ois = new SafeObjectInputStream(bais, allowedClasses);
      return (T) ois.readObject();
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException("[FQDataType] Failed to deserialize object: " + e.getMessage(), e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public T[] createStorage(int size) {
    return (T[]) new Serializable[size];
  }
}
