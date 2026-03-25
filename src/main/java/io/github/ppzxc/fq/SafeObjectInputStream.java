package io.github.ppzxc.fq;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.util.Set;

/**
 * A custom {@link ObjectInputStream} that enforces a class whitelist during deserialization
 * to mitigate Java deserialization vulnerabilities (CWE-502).
 *
 * <p>When the allowedClasses set is empty, all classes are permitted (backward-compatible mode).
 * When non-empty, only the specified class names are allowed; any other class triggers an
 * {@link IllegalArgumentException}.</p>
 */
class SafeObjectInputStream extends ObjectInputStream {

  private final Set<String> allowedClasses;

  SafeObjectInputStream(InputStream in, Set<String> allowedClasses) throws IOException {
    super(in);
    this.allowedClasses = allowedClasses;
  }

  @Override
  protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
    String className = desc.getName();
    if (!allowedClasses.isEmpty() && !isAllowed(className)) {
      throw new IllegalArgumentException("Deserialization of class not allowed: " + className);
    }
    return super.resolveClass(desc);
  }

  private boolean isAllowed(String className) {
    if (allowedClasses.contains(className)) {
      return true;
    }
    // Allow arrays of allowed component types, e.g. "[Ljava.lang.String;"
    if (className.startsWith("[L") && className.endsWith(";")) {
      String componentType = className.substring(2, className.length() - 1);
      return isAllowed(componentType);
    }
    // Allow primitive arrays ([B, [I, [J, [D, [F, [Z, [S, [C)
    return className.length() == 2 && className.startsWith("[");
  }
}
