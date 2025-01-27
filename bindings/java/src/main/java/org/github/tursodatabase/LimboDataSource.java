package org.github.tursodatabase;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.github.tursodatabase.annotations.Nullable;
import org.github.tursodatabase.annotations.SkipNullableCheck;

/** Provides {@link DataSource} API for configuring Limbo database connection. */
public class LimboDataSource implements DataSource {

  private final LimboConfig limboConfig;
  private final String url;

  /**
   * Creates a datasource based on the provided configuration.
   *
   * @param limboConfig The configuration for the datasource.
   */
  public LimboDataSource(LimboConfig limboConfig, String url) {
    this.limboConfig = limboConfig;
    this.url = url;
  }

  @Override
  @Nullable
  public Connection getConnection() throws SQLException {
    return getConnection(null, null);
  }

  @Override
  @Nullable
  public Connection getConnection(@Nullable String username, @Nullable String password)
      throws SQLException {
    Properties properties = limboConfig.toProperties();
    if (username != null) properties.put("user", username);
    if (password != null) properties.put("pass", password);
    return JDBC.createConnection(url, properties);
  }

  @Override
  @SkipNullableCheck
  public PrintWriter getLogWriter() throws SQLException {
    // TODO
    return null;
  }

  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
    // TODO
  }

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
    // TODO
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    // TODO
    return 0;
  }

  @Override
  @SkipNullableCheck
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    // TODO
    return null;
  }

  @Override
  @SkipNullableCheck
  public <T> T unwrap(Class<T> iface) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    // TODO
    return false;
  }
}
