package org.github.tursodatabase.core;


import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

/**
 * This class provides a thin JNI layer over the SQLite3 C API.
 */
public final class LimboDB extends DB {
    /**
     * SQLite connection handle.
     */
    private long pointer = 0;

    private static boolean isLoaded;
    private static boolean loadSucceeded;

    static {
        if ("The Android Project".equals(System.getProperty("java.vm.vendor"))) {
            System.loadLibrary("sqlitejdbc");
            isLoaded = true;
            loadSucceeded = true;
        } else {
            // continue with non Android execution path
            isLoaded = false;
            loadSucceeded = false;
        }
    }

    // TODO: receive config as argument
    public LimboDB(String url, String fileName) throws SQLException {
        super(url, fileName);
    }

    /**
     * Loads the SQLite interface backend.
     *
     * @return True if the SQLite JDBC driver is successfully loaded; false otherwise.
     */
    public static boolean load() throws Exception {
        if (isLoaded) return loadSucceeded;

        try {
            System.loadLibrary("_limbo_java");
            loadSucceeded = true;
        } finally {
            isLoaded = true;
        }
        return loadSucceeded;
    }

    // WRAPPER FUNCTIONS ////////////////////////////////////////////

    @Override
    protected synchronized void _open(String file, int openFlags) throws SQLException {
        // TODO: add implementation
        throw new SQLFeatureNotSupportedException();
    }

    // TODO: add support for JNI
    synchronized native void _open_utf8(byte[] fileUtf8, int openFlags) throws SQLException;

    // TODO: add support for JNI
    @Override
    protected synchronized native void _close() throws SQLException;

    @Override
    public synchronized int _exec(String sql) throws SQLException {
        // TODO: add implementation
        throw new SQLFeatureNotSupportedException();
    }

    // TODO: add support for JNI
    synchronized native int _exec_utf8(byte[] sqlUtf8) throws SQLException;

    // TODO: add support for JNI
    @Override
    public native void interrupt();

    @Override
    protected synchronized SafeStmtPtr prepare(String sql) throws SQLException {
        // TODO: add implementation
        throw new SQLFeatureNotSupportedException();
    }

    // TODO: add support for JNI
    @Override
    protected synchronized native int finalize(long stmt);

    // TODO: add support for JNI
    @Override
    public synchronized native int step(long stmt);
}
