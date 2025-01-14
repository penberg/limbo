package org.github.tursodatabase;

import org.github.tursodatabase.core.AbstractDB;
import org.github.tursodatabase.core.LimboDB;

import java.sql.Connection;
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
        if (fileName.isBlank()) {
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
}
