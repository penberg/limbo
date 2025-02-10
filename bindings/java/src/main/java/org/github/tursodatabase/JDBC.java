package org.github.tursodatabase;

import java.sql.*;
import java.util.Locale;
import java.util.Properties;
import org.github.tursodatabase.annotations.Nullable;
import org.github.tursodatabase.annotations.SkipNullableCheck;
import org.github.tursodatabase.jdbc4.JDBC4Connection;
import org.github.tursodatabase.utils.Logger;
import org.github.tursodatabase.utils.LoggerFactory;

public class JDBC implements Driver {
  private static final Logger logger = LoggerFactory.getLogger(JDBC.class);

  private static final String VALID_URL_PREFIX = "jdbc:sqlite:";

  static {
    try {
      DriverManager.registerDriver(new JDBC());
    } catch (Exception e) {
      logger.error("Failed to register driver", e);
    }
  }

  @Nullable
  public static JDBC4Connection createConnection(String url, Properties properties)
      throws SQLException {
    if (!isValidURL(url)) return null;

    url = url.trim();
    return new JDBC4Connection(url, extractAddress(url), properties);
  }

  private static boolean isValidURL(String url) {
    return url != null && url.toLowerCase(Locale.ROOT).startsWith(VALID_URL_PREFIX);
  }

  private static String extractAddress(String url) {
    return url.substring(VALID_URL_PREFIX.length());
  }

  @Nullable
  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    return createConnection(url, info);
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return isValidURL(url);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    return LimboConfig.getDriverPropertyInfo();
  }

  @Override
  public int getMajorVersion() {
    // TODO
    return 0;
  }

  @Override
  public int getMinorVersion() {
    // TODO
    return 0;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  @SkipNullableCheck
  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
    // TODO
    return null;
  }
}
