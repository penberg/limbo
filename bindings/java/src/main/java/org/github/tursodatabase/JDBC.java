package org.github.tursodatabase;

import org.github.tursodatabase.jdbc4.JDBC4Connection;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class JDBC implements Driver {
    private static final String VALID_URL_PREFIX = "jdbc:limbo:";

    static {
        try {
            DriverManager.registerDriver(new JDBC());
        } catch (Exception e) {
            // TODO: log
        }
    }

    public static LimboConnection createConnection(String url, Properties properties) throws SQLException {
        if (!isValidURL(url)) return null;

        url = url.trim();
        return new JDBC4Connection(url, extractAddress(url), properties);
    }

    private static boolean isValidURL(String url) {
        return url != null && url.toLowerCase().startsWith(VALID_URL_PREFIX);
    }

    private static String extractAddress(String url) {
        return url.substring(VALID_URL_PREFIX.length());
    }

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
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        // TODO
        return null;
    }
}
