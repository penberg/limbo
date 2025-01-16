package org.github.tursodatabase.core;

import org.github.tursodatabase.annotations.Nullable;
import org.github.tursodatabase.jdbc4.JDBC4ResultSet;

import java.sql.SQLException;

public abstract class CoreStatement {

    protected final LimboConnection connection;
    protected final CoreResultSet resultSet;

    @Nullable
    protected String sql = null;
    @Nullable
    private SafeStatementPointer stmtPointer;

    protected boolean resultsWaiting = false;

    protected CoreStatement(LimboConnection connection) {
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

    @Nullable
    public SafeStatementPointer getStmtPointer() {
        return this.stmtPointer;
    }

    public void setStmtPointer(SafeStatementPointer stmtPointer) {
        this.stmtPointer = stmtPointer;
    }

    /**
     * Calls sqlite3_step() and sets up results.
     *
     * @return true if the ResultSet has at least one row; false otherwise;
     * @throws SQLException If the given SQL statement is nul or no database is open;
     */
    protected boolean exec() throws SQLException {
        if (sql == null) throw new SQLException("SQL must not be null");
        if (stmtPointer == null) throw new SQLException("stmtPointer must not be null");
        if (resultSet.isOpen()) throw new SQLException("ResultSet is open on exec");

        boolean success = false;
        boolean result = false;

        try {
            result = connection.getDatabase().execute(this, null);
            success = true;
        } finally {
            resultsWaiting = result;
            if (!success) {
                this.stmtPointer.close();
            }
        }

        return this.stmtPointer.columnCount() != 0;
    }
}
