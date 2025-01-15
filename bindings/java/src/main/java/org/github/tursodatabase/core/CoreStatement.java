package org.github.tursodatabase.core;

import org.github.tursodatabase.LimboConnection;

import java.sql.SQLException;

public abstract class CoreStatement {

    private final LimboConnection connection;

    protected CoreStatement(LimboConnection connection) {
        this.connection = connection;
    }

    protected void internalClose() throws SQLException {
        // TODO
    }

    protected void clearGeneratedKeys() throws SQLException {
        // TODO
    }

    protected void updateGeneratedKeys() throws SQLException {
        // TODO
    }
}
