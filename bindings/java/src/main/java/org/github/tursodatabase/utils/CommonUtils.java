package org.github.tursodatabase.utils;

import org.github.tursodatabase.annotations.Nullable;

public class CommonUtils {

  public static <T> T requireNotNull(@Nullable T obj) {
    return requireNotNull(obj, obj + " must not be null");
  }

  public static <T> T requireNotNull(@Nullable T obj, String message) {
    if (obj != null) return obj;
    throw new IllegalArgumentException(message);
  }
}
