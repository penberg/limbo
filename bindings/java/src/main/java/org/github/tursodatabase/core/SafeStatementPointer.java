package org.github.tursodatabase.core;

import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A class for safely wrapping calls to a native pointer to a statement.
 * Ensures that no other thread has access to the pointer while it is running.
 */
public class SafeStatementPointer {

    // Store a reference to database, so we can lock it before calling any safe functions.
    private final LimboConnection connection;
    private final long statementPtr;

    private volatile boolean closed = false;

    private final ReentrantLock connectionLock = new ReentrantLock();

    public SafeStatementPointer(LimboConnection connection, long statementPtr) {
        this.connection = connection;
        this.statementPtr = statementPtr;
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
            connectionLock.lock();
            return internalClose();
        } finally {
            connectionLock.unlock();
        }
    }

    private int internalClose() throws SQLException {
        // TODO
        return 0;
    }

    public long columnCount() throws SQLException {
        // TODO
        return 0;
    }
}
