package tech.turso.utils;

import java.util.logging.Level;

/**
 * A factory for {@link Logger} instances that uses SLF4J if present, falling back on a
 * java.util.logging implementation otherwise.
 */
public class LoggerFactory {
  static final boolean USE_SLF4J;

  static {
    boolean useSLF4J;
    try {
      Class.forName("org.slf4j.Logger");
      useSLF4J = true;
    } catch (Exception e) {
      useSLF4J = false;
    }
    USE_SLF4J = useSLF4J;
  }

  /**
   * Get a {@link Logger} instance for the given host class.
   *
   * @param hostClass the host class from which log messages will be issued
   * @return a Logger
   */
  public static Logger getLogger(Class<?> hostClass) {
    if (USE_SLF4J) {
      return new SLF4JLogger(hostClass);
    }

    return new JDKLogger(hostClass);
  }

  private static class JDKLogger implements Logger {
    final java.util.logging.Logger logger;

    public JDKLogger(Class<?> hostClass) {
      logger = java.util.logging.Logger.getLogger(hostClass.getCanonicalName());
    }

    @Override
    public void trace(String message, Object... params) {
      if (logger.isLoggable(Level.FINEST)) {
        logger.log(Level.FINEST, String.format(message, params));
      }
    }

    @Override
    public void debug(String message, Object... params) {
      if (logger.isLoggable(Level.FINE)) {
        logger.log(Level.FINE, String.format(message, params));
      }
    }

    @Override
    public void info(String message, Object... params) {
      if (logger.isLoggable(Level.INFO)) {
        logger.log(Level.INFO, String.format(message, params));
      }
    }

    @Override
    public void warn(String message, Object... params) {
      if (logger.isLoggable(Level.WARNING)) {
        logger.log(Level.WARNING, String.format(message, params));
      }
    }

    @Override
    public void error(String message, Object... params) {
      if (logger.isLoggable(Level.SEVERE)) {
        logger.log(Level.SEVERE, String.format(message, params));
      }
    }
  }

  private static class SLF4JLogger implements Logger {
    final org.slf4j.Logger logger;

    SLF4JLogger(Class<?> hostClass) {
      logger = org.slf4j.LoggerFactory.getLogger(hostClass);
    }

    @Override
    public void trace(String message, Object... params) {
      if (logger.isTraceEnabled()) {
        logger.trace(message, String.format(message, params));
      }
    }

    @Override
    public void debug(String message, Object... params) {
      if (logger.isDebugEnabled()) {
        logger.debug(message, String.format(message, params));
      }
    }

    @Override
    public void info(String message, Object... params) {
      if (logger.isInfoEnabled()) {
        logger.info(message, String.format(message, params));
      }
    }

    @Override
    public void warn(String message, Object... params) {
      if (logger.isWarnEnabled()) {
        logger.warn(message, String.format(message, params));
      }
    }

    @Override
    public void error(String message, Object... params) {
      if (logger.isErrorEnabled()) {
        logger.error(message, String.format(message, params));
      }
    }
  }
}
