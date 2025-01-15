package org.github.tursodatabase;

import org.github.tursodatabase.core.AbstractDB;
import org.github.tursodatabase.core.LimboDB;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public abstract class LimboConnection implements Connection {

    private final AbstractDB database;

    public LimboConnection(AbstractDB database) {
        this.database = database;
    }

    public LimboConnection(String url, String fileName) throws SQLException {
        this(url, fileName, new Properties());
    }

    /**
     * Creates a connection to limbo database.
     *
     * @param url      e.g. "jdbc:sqlite:fileName"
     * @param fileName path to file
     */
    public LimboConnection(String url, String fileName, Properties properties) throws SQLException {
        AbstractDB db = null;

        try {
            db = open(url, fileName, properties);
        } catch (Throwable t) {
            try {
                if (db != null) {
                    db.close();
                }
            } catch (Throwable t2) {
                t.addSuppressed(t2);
            }

            throw t;
        }

        this.database = db;
    }

    private static AbstractDB open(String url, String fileName, Properties properties) throws SQLException {
        if (fileName.isEmpty()) {
            throw new IllegalArgumentException("fileName should not be empty");
        }

        final AbstractDB database;
        try {
            LimboDB.load();
            database = LimboDB.create(url, fileName);
        } catch (Exception e) {
            throw new SQLException("Error opening connection", e);
        }

        database.open(0);
        return database;
    }

    protected void checkOpen() throws SQLException {
        if (isClosed()) throw new SQLException("database connection closed");
    }

    @Override
    public void close() throws SQLException {
        if (isClosed()) return;
        database.close();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return database.isClosed();
    }

    // TODO: check whether this is still valid for limbo
    /**
     * Checks whether the type, concurrency, and holdability settings for a {@link ResultSet} are
     * supported by the SQLite interface. Supported settings are:
     *
     * <ul>
     *   <li>type: {@link ResultSet#TYPE_FORWARD_ONLY}
     *   <li>concurrency: {@link ResultSet#CONCUR_READ_ONLY})
     *   <li>holdability: {@link ResultSet#CLOSE_CURSORS_AT_COMMIT}
     * </ul>
     *
     * @param resultSetType        the type setting.
     * @param resultSetConcurrency the concurrency setting.
     * @param resultSetHoldability the holdability setting.
     */
    protected void checkCursor(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY)
            throw new SQLException("SQLite only supports TYPE_FORWARD_ONLY cursors");
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY)
            throw new SQLException("SQLite only supports CONCUR_READ_ONLY cursors");
        if (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT)
            throw new SQLException("SQLite only supports closing cursors at commit");
    }
}
