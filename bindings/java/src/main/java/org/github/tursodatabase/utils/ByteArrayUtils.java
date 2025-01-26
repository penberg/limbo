package org.github.tursodatabase.utils;

import java.nio.charset.StandardCharsets;
import org.github.tursodatabase.annotations.Nullable;

public class ByteArrayUtils {
  @Nullable
  public static String utf8ByteBufferToString(@Nullable byte[] buffer) {
    if (buffer == null) {
      return null;
    }

    return new String(buffer, StandardCharsets.UTF_8);
  }

  @Nullable
  public static byte[] stringToUtf8ByteArray(@Nullable String str) {
    if (str == null) {
      return null;
    }
    return str.getBytes(StandardCharsets.UTF_8);
  }
}
