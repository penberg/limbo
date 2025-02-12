package tech.turso;

import java.io.IOException;
import java.nio.file.Files;

public class TestUtils {
  /** Create temporary file and returns the path. */
  public static String createTempFile() throws IOException {
    return Files.createTempFile("limbo_test_db", null).toAbsolutePath().toString();
  }
}
