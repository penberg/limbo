package tech.turso.jdbc4;

import static java.util.Objects.requireNonNull;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.concurrent.locks.ReentrantLock;
import tech.turso.annotations.Nullable;
import tech.turso.annotations.SkipNullableCheck;
import tech.turso.core.LimboResultSet;
import tech.turso.core.LimboStatement;

public class JDBC4Statement implements Statement {

  private final JDBC4Connection connection;
  @Nullable protected LimboStatement statement = null;

  // Because JDBC4Statement has different life cycle in compared to LimboStatement, let's use this
  // field to manage JDBC4Statement lifecycle
  private boolean closed;
  private boolean closeOnCompletion;

  private final int resultSetType;
  private final int resultSetConcurrency;
  private final int resultSetHoldability;

  private int queryTimeoutSeconds;
  private long updateCount;
  private boolean exhaustedResults = false;

  private ReentrantLock connectionLock = new ReentrantLock();

  public JDBC4Statement(JDBC4Connection connection) {
    this(
        connection,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.CLOSE_CURSORS_AT_COMMIT);
  }

  public JDBC4Statement(
      JDBC4Connection connection,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability) {
    this.connection = connection;
    this.resultSetType = resultSetType;
    this.resultSetConcurrency = resultSetConcurrency;
    this.resultSetHoldability = resultSetHoldability;
  }

  // TODO: should executeQuery run execute right after preparing the statement?
  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    ensureOpen();
    statement =
        this.withConnectionTimeout(
            () -> {
              try {
                // TODO: if sql is a readOnly query, do we still need the locks?
                connectionLock.lock();
                return connection.prepare(sql);
              } finally {
                connectionLock.unlock();
              }
            });

    requireNonNull(statement, "statement should not be null after running execute method");
    return new JDBC4ResultSet(statement.getResultSet());
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    execute(sql);

    requireNonNull(statement, "statement should not be null after running execute method");
    final LimboResultSet resultSet = statement.getResultSet();
    resultSet.consumeAll();

    // TODO: return update count;
    return 0;
  }

  @Override
  public void close() throws SQLException {
    if (closed) {
      return;
    }

    if (this.statement != null) {
      this.statement.close();
    }

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

  /**
   * The <code>execute</code> method executes an SQL statement and indicates the form of the first
   * result. You must then use the methods <code>getResultSet</code> or <code>getUpdateCount</code>
   * to retrieve the result, and <code>getMoreResults</code> to move to any subsequent result(s).
   */
  @Override
  public boolean execute(String sql) throws SQLException {
    ensureOpen();
    return this.withConnectionTimeout(
        () -> {
          try {
            // TODO: if sql is a readOnly query, do we still need the locks?
            connectionLock.lock();
            statement = connection.prepare(sql);
            final boolean result = statement.execute();
            updateGeneratedKeys();
            exhaustedResults = false;

            return result;
          } finally {
            connectionLock.unlock();
          }
        });
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    requireNonNull(statement, "statement is null");
    return new JDBC4ResultSet(statement.getResultSet());
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
  public Connection getConnection() {
    return connection;
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
    // TODO: enhance
    return executeUpdate(sql);
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    // TODO: enhance
    return executeUpdate(sql);
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    // TODO: enhance
    return executeUpdate(sql);
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    // TODO: enhance
    return execute(sql);
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    // TODO: enhance
    return execute(sql);
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
    return this.closed;
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
    if (closed) {
      throw new SQLException("statement is closed");
    }
    closeOnCompletion = true;
  }

  /**
   * Indicates whether the statement should be closed automatically when all its dependent result
   * sets are closed.
   */
  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    if (closed) {
      throw new SQLException("statement is closed");
    }
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

  protected void updateGeneratedKeys() throws SQLException {
    // TODO
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

  private void ensureOpen() throws SQLException {
    if (closed) {
      throw new SQLException("Statement is closed");
    }
  }
}
