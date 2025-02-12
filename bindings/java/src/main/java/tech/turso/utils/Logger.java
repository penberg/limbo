package tech.turso.utils;

/** A simple internal Logger interface. */
public interface Logger {
  void trace(String message, Object... params);

  void debug(String message, Object... params);

  void info(String message, Object... params);

  void warn(String message, Object... params);

  void error(String message, Object... params);
}
