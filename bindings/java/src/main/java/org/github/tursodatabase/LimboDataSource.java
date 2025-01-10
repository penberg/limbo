package org.github.tursodatabase;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Provides {@link DataSource} API for configuring Limbo database connection.
 */
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
    public Connection getConnection() throws SQLException {
        return getConnection(null, null);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        Properties properties = limboConfig.toProperties();
        if (username != null) properties.put("user", username);
        if (password != null) properties.put("pass", password);
        return JDBC.createConnection(url, properties);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {

    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {

    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
