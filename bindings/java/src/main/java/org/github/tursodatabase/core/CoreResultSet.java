package org.github.tursodatabase.core;

import java.sql.SQLException;

/**
 * JDBC ResultSet.
 */
public abstract class CoreResultSet {

    protected final CoreStatement statement;

    // Whether the result set does not have any rows.
    protected boolean isEmptyResultSet = false;
    // If the result set is open. Doesn't mean it has results.
    protected boolean isOpen = false;
    // Maximum number of rows as set by the statement
    protected long maxRows;
    // number of current row, starts at 1 (0 is used to represent loading data)
    protected int row = 0;

    protected CoreResultSet(CoreStatement statement) {
        this.statement = statement;
    }

    /**
     * Checks the status of the result set.
     *
     * @return true if it's ready to iterate over the result set; false otherwise.
     */
    public boolean isOpen() {
        return isOpen;
    }

    /**
     * @throws SQLException if not {@link #isOpen}
     */
    protected void checkOpen() throws SQLException {
        if (!isOpen) {
            throw new SQLException("ResultSet closed");
        }
    }
}
