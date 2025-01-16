package org.github.tursodatabase.core;

import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A class for safely wrapping calls to a native pointer to a statement.
 * Ensures that no other thread has access to the pointer while it is running.
 */
public class SafeStatementPointer {

    // Store a reference to database, so we can lock it before calling any safe functions.
    private final AbstractDB database;
    private final long databasePointer;

    private volatile boolean closed = false;

    private final ReentrantLock databaseLock = new ReentrantLock();

    public SafeStatementPointer(AbstractDB database, long databasePointer) {
        this.database = database;
        this.databasePointer = databasePointer;
    }

    /**
     * Whether this safe pointer has been closed.
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Close the pointer.
     *
     * @return the return code of the close callback function
     */
    public int close() throws SQLException {
        try {
            databaseLock.lock();
            return internalClose();
        } finally {
            databaseLock.unlock();
        }
    }

    private int internalClose() throws SQLException {
        // TODO
        return 0;
    }

    public <E extends Throwable> int safeRunInt(SafePointerIntFunction<E> function) throws SQLException, E {
        try {
            databaseLock.lock();
            this.ensureOpen();
            return function.run(database, databasePointer);
        } finally {
            databaseLock.unlock();
        }
    }

    private void ensureOpen() throws SQLException {
        if (this.closed) {
            throw new SQLException("Pointer is closed");
        }
    }

    @FunctionalInterface
    public interface SafePointerIntFunction<E extends Throwable> {
        int run(AbstractDB database, long pointer) throws E;
    }
}
