package org.github.tursodatabase.core;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Interface to Limbo. It provides some helper functions used by other parts of the driver. The goal
 * of the helper functions here are not only to provide functionality, but to handle contractual
 * differences between the JDBC specification and the Limbo API.
 */
public abstract class AbstractDB {
  protected final String url;
  protected final String filePath;
  private final AtomicBoolean closed = new AtomicBoolean(true);

  public AbstractDB(String url, String filePath) {
    this.url = url;
    this.filePath = filePath;
  }

  public boolean isClosed() {
    return closed.get();
  }

  /** Aborts any pending operation and returns at its earliest opportunity. */
  public abstract void interrupt() throws SQLException;

  /**
   * Creates an SQLite interface to a database for the given connection.
   *
   * @param openFlags Flags for opening the database.
   * @throws SQLException if a database access error occurs.
   */
  public final synchronized void open(int openFlags) throws SQLException {
    open0(filePath, openFlags);
  }

  protected abstract void open0(String fileName, int openFlags) throws SQLException;

  /**
   * Closes a database connection and finalizes any remaining statements before the closing
   * operation.
   *
   * @throws SQLException if a database access error occurs.
   */
  public final synchronized void close() throws SQLException {
    // TODO: add implementation
    throw new SQLFeatureNotSupportedException();
  }

  /**
   * Connects to a database.
   *
   * @return Pointer to the connection.
   */
  public abstract long connect() throws SQLException;

  /**
   * Creates an SQLite interface to a database with the provided open flags.
   *
   * @param fileName The database to open.
   * @param openFlags Flags for opening the database.
   * @return pointer to database instance
   * @throws SQLException if a database access error occurs.
   */
  protected abstract long openUtf8(byte[] fileName, int openFlags) throws SQLException;

  /**
   * Closes the SQLite interface to a database.
   *
   * @throws SQLException if a database access error occurs.
   */
  protected abstract void close0() throws SQLException;
}
