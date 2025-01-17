package org.github.tursodatabase.jdbc4;

import org.github.tursodatabase.annotations.SkipNullableCheck;
import org.github.tursodatabase.core.LimboConnection;
import org.github.tursodatabase.core.LimboStatement;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Implementation of the {@link Statement} interface for JDBC 4.
 */
public class JDBC4Statement extends LimboStatement implements Statement {

    private boolean closed;
    private boolean closeOnCompletion;

    private final int resultSetType;
    private final int resultSetConcurrency;
    private final int resultSetHoldability;

    private int queryTimeoutSeconds;
    private long updateCount;
    private boolean exhaustedResults = false;

    private ReentrantLock connectionLock = new ReentrantLock();

    public JDBC4Statement(LimboConnection connection) {
        this(connection, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    public JDBC4Statement(LimboConnection connection, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        super(connection);
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.resultSetHoldability = resultSetHoldability;
    }

    @Override
    @SkipNullableCheck
    public ResultSet executeQuery(String sql) throws SQLException {
        // TODO
        return null;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public void close() throws SQLException {
        clearGeneratedKeys();
        internalClose();
        closed = true;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        // TODO
    }

    @Override
    public int getMaxRows() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        // TODO
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        // TODO
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        if (seconds < 0) {
            throw new SQLException("Query timeout must be greater than 0");
        }
        this.queryTimeoutSeconds = seconds;
    }

    @Override
    public void cancel() throws SQLException {
        // TODO
    }

    @Override
    @SkipNullableCheck
    public SQLWarning getWarnings() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        // TODO
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        // TODO
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        internalClose();

        return this.withConnectionTimeout(
                () -> {
                    try {
                        connectionLock.lock();
                        final long stmtPointer = connection.prepare(sql);
                        List<Object[]> result = execute(stmtPointer);
                        updateGeneratedKeys();
                        exhaustedResults = false;
                        return !result.isEmpty();
                    } finally {
                        connectionLock.unlock();
                    }
                }
        );
    }

    @Override
    @SkipNullableCheck
    public ResultSet getResultSet() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        // TODO
    }

    @Override
    public int getFetchDirection() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        // TODO
    }

    @Override
    public int getFetchSize() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public int getResultSetConcurrency() {
        return resultSetConcurrency;
    }

    @Override
    public int getResultSetType() {
        return resultSetType;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        // TODO
    }

    @Override
    public void clearBatch() throws SQLException {
        // TODO
    }

    @Override
    public int[] executeBatch() throws SQLException {
        // TODO
        return new int[0];
    }

    @Override
    @SkipNullableCheck
    public Connection getConnection() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        // TODO
        return false;
    }

    @Override
    @SkipNullableCheck
    public ResultSet getGeneratedKeys() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        // TODO
        return false;
    }

    @Override
    public int getResultSetHoldability() {
        return resultSetHoldability;
    }

    @Override
    public boolean isClosed() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        // TODO
    }

    @Override
    public boolean isPoolable() throws SQLException {
        // TODO
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        if (closed) throw new SQLException("statement is closed");
        closeOnCompletion = true;
    }

    /**
     * Indicates whether the statement should be closed automatically when all its dependent result sets are closed.
     */
    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        if (closed) throw new SQLException("statement is closed");
        return closeOnCompletion;
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

    private <T> T withConnectionTimeout(SQLCallable<T> callable) throws SQLException {
        final int originalBusyTimeoutMillis = connection.getBusyTimeout();
        if (queryTimeoutSeconds > 0) {
            // TODO: set busy timeout
            connection.setBusyTimeout(1000 * queryTimeoutSeconds);
        }

        try {
            return callable.call();
        } finally {
            if (queryTimeoutSeconds > 0) {
                connection.setBusyTimeout(originalBusyTimeoutMillis);
            }
        }
    }

    @FunctionalInterface
    protected interface SQLCallable<T> {
        T call() throws SQLException;
    }
}
