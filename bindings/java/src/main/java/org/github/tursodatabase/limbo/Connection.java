package org.github.tursodatabase.limbo;

import java.lang.Exception;

/**
 * Represents a connection to the database.
 */
public class Connection {

    // Pointer to the connection object
    private final long connectionPtr;

    public Connection(long connectionPtr) {
        this.connectionPtr = connectionPtr;
    }

    /**
     * Creates a new cursor object using this connection.
     *
     * @return A new Cursor object.
     * @throws Exception If the cursor cannot be created.
     */
    public Cursor cursor() throws Exception {
        long cursorId = cursor(connectionPtr);
        return new Cursor(cursorId);
    }

    private native long cursor(long connectionPtr);

    /**
     * Closes the connection to the database.
     *
     * @throws Exception If there is an error closing the connection.
     */
    public void close() throws Exception {
        close(connectionPtr);
    }

    private native void close(long connectionPtr);

    /**
     * Commits the current transaction.
     *
     * @throws Exception If there is an error during commit.
     */
    public void commit() throws Exception {
        try {
            commit(connectionPtr);
        } catch (Exception e) {
            System.out.println("caught exception: " + e);
        }
    }

    private native void commit(long connectionPtr) throws Exception;

    /**
     * Rolls back the current transaction.
     *
     * @throws Exception If there is an error during rollback.
     */
    public void rollback() throws Exception {
        rollback(connectionPtr);
    }

    private native void rollback(long connectionPtr) throws Exception;
}
