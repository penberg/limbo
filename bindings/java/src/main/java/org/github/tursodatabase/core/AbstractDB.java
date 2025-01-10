package org.github.tursodatabase.core;

import org.github.tursodatabase.LimboErrorCode;
import org.github.tursodatabase.NativeInvocation;
import org.github.tursodatabase.exceptions.LimboException;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Interface to Limbo. It provides some helper functions
 * used by other parts of the driver. The goal of the helper functions here
 * are not only to provide functionality, but to handle contractual
 * differences between the JDBC specification and the Limbo API.
 */
public abstract class AbstractDB {
    private final String url;
    private final String fileName;
    private final AtomicBoolean closed = new AtomicBoolean(true);

    public AbstractDB(String url, String filaName) throws SQLException {
        this.url = url;
        this.fileName = filaName;
    }

    public boolean isClosed() {
        return closed.get();
    }

    /**
     * Aborts any pending operation and returns at its earliest opportunity.
     */
    public abstract void interrupt() throws SQLException;

    /**
     * Executes an SQL statement.
     *
     * @param sql        SQL statement to be executed.
     * @param autoCommit Whether to auto-commit the transaction.
     * @throws SQLException if a database access error occurs.
     */
    public final synchronized void exec(String sql, boolean autoCommit) throws SQLException {
        // TODO: add implementation
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * Creates an SQLite interface to a database for the given connection.
     *
     * @param openFlags Flags for opening the database.
     * @throws SQLException if a database access error occurs.
     */
    public final synchronized void open(int openFlags) throws SQLException {
        _open(fileName, openFlags);
    }

    protected abstract void _open(String fileName, int openFlags) throws SQLException;

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
     * Compiles an SQL statement.
     *
     * @param stmt The SQL statement to compile.
     * @throws SQLException if a database access error occurs.
     */
    public final synchronized void prepare(CoreStatement stmt) throws SQLException {
        // TODO: add implementation
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * Destroys a statement.
     *
     * @param safePtr the pointer wrapper to remove from internal structures.
     * @param ptr     the raw pointer to free.
     * @return <a href="https://www.sqlite.org/c3ref/c_abort.html">Result Codes</a>
     * @throws SQLException if a database access error occurs.
     */
    public synchronized int finalize(SafeStmtPtr safePtr, long ptr) throws SQLException {
        // TODO: add implementation
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * Creates an SQLite interface to a database with the provided open flags.
     *
     * @param fileName  The database to open.
     * @param openFlags Flags for opening the database.
     * @return pointer to database instance
     * @throws SQLException if a database access error occurs.
     */
    protected abstract long _open_utf8(byte[] fileName, int openFlags) throws SQLException;

    /**
     * Closes the SQLite interface to a database.
     *
     * @throws SQLException if a database access error occurs.
     */
    protected abstract void _close() throws SQLException;

    /**
     * Compiles, evaluates, executes and commits an SQL statement.
     *
     * @param sql An SQL statement.
     * @return Result code.
     * @throws SQLException if a database access error occurs.
     */
    public abstract int _exec(String sql) throws SQLException;

    /**
     * Compiles an SQL statement.
     *
     * @param sql An SQL statement.
     * @return A SafeStmtPtr object.
     * @throws SQLException if a database access error occurs.
     */
    protected abstract SafeStmtPtr prepare(String sql) throws SQLException;

    /**
     * Destroys a prepared statement.
     *
     * @param stmt Pointer to the statement pointer.
     * @return Result code.
     * @throws SQLException if a database access error occurs.
     */
    protected abstract int finalize(long stmt) throws SQLException;

    /**
     * Evaluates a statement.
     *
     * @param stmt Pointer to the statement.
     * @return Result code.
     * @throws SQLException if a database access error occurs.
     */
    public abstract int step(long stmt) throws SQLException;

    /**
     * Executes a statement with the provided parameters.
     *
     * @param stmt Stmt object.
     * @param vals Array of parameter values.
     * @return True if a row of ResultSet is ready; false otherwise.
     * @throws SQLException if a database access error occurs.
     * @see <a href="https://www.sqlite.org/c_interface.html#sqlite_exec">SQLite Exec</a>
     */
    public final synchronized boolean execute(CoreStatement stmt, Object[] vals) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * Executes an SQL INSERT, UPDATE or DELETE statement with the Stmt object and an array of
     * parameter values of the SQL statement.
     *
     * @param stmt Stmt object.
     * @param vals Array of parameter values.
     * @return Number of database rows that were changed or inserted or deleted by the most recently
     * completed SQL.
     * @throws SQLException if a database access error occurs.
     */
    public final synchronized long executeUpdate(CoreStatement stmt, Object[] vals) throws SQLException {
        // TODO: add implementation
        throw new SQLFeatureNotSupportedException();
    }
}
