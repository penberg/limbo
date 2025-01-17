package org.github.tursodatabase.core;

import org.github.tursodatabase.annotations.Nullable;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Interface to Limbo. It provides some helper functions
 * used by other parts of the driver. The goal of the helper functions here
 * are not only to provide functionality, but to handle contractual
 * differences between the JDBC specification and the Limbo API.
 */
public abstract class AbstractDB {
    protected final String url;
    protected final String filePath;
    private final AtomicBoolean closed = new AtomicBoolean(true);

    // Tracer for statements to avoid unfinalized statements on db close.
    private final Set<SafeStatementPointer> statementPointerSet = ConcurrentHashMap.newKeySet();

    public AbstractDB(String url, String filePath) {
        this.url = url;
        this.filePath = filePath;
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
     * Destroys a statement.
     *
     * @param safePtr the pointer wrapper to remove from internal structures.
     * @param ptr     the raw pointer to free.
     * @return <a href="https://www.sqlite.org/c3ref/c_abort.html">Result Codes</a>
     * @throws SQLException if a database access error occurs.
     */
    public synchronized int finalize(SafeStatementPointer safePtr, long ptr) throws SQLException {
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
    protected abstract long openUtf8(byte[] fileName, int openFlags) throws SQLException;

    /**
     * Closes the SQLite interface to a database.
     *
     * @throws SQLException if a database access error occurs.
     */
    protected abstract void close0() throws SQLException;

    /**
     * Compiles, evaluates, executes and commits an SQL statement.
     *
     * @param sql An SQL statement.
     * @return Result code.
     * @throws SQLException if a database access error occurs.
     */
    public abstract int exec(String sql) throws SQLException;

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
    public final synchronized boolean execute(CoreStatement stmt, @Nullable Object[] vals) throws SQLException {
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
