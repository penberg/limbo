package org.github.tursodatabase.core;

import org.github.tursodatabase.annotations.Nullable;
import org.github.tursodatabase.jdbc4.JDBC4ResultSet;

import java.sql.SQLException;

public abstract class LimboStatement {

    protected final LimboConnection connection;
    protected final LimboResultSet resultSet;

    @Nullable
    protected String sql = null;

    protected LimboStatement(LimboConnection connection) {
        this.connection = connection;
        this.resultSet = new JDBC4ResultSet(this);
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

    /**
     * Calls sqlite3_step() and sets up results.
     *
     * @return true if the ResultSet has at least one row; false otherwise;
     * @throws SQLException If the given SQL statement is nul or no database is open;
     */
    protected boolean exec(long stmtPointer) throws SQLException {
        if (sql == null) throw new SQLException("SQL must not be null");

        // TODO
        return true;
    }

    protected void step(long stmtPointer) throws SQLException {
        // TODO
    }
}
