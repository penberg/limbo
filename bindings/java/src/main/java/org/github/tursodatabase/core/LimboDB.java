package org.github.tursodatabase.core;


import org.github.tursodatabase.LimboErrorCode;
import org.github.tursodatabase.NativeInvocation;
import org.github.tursodatabase.exceptions.LimboException;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

/**
 * This class provides a thin JNI layer over the SQLite3 C API.
 */
public final class LimboDB extends AbstractDB {

    // Pointer to database instance
    private long dbPtr;
    private boolean isOpen;

    private static boolean isLoaded;

    static {
        if ("The Android Project".equals(System.getProperty("java.vm.vendor"))) {
            // TODO
        } else {
            // continue with non Android execution path
            isLoaded = false;
        }
    }

    // url example: "jdbc:sqlite:{fileName}

    /**
     * @param url      e.g. "jdbc:sqlite:fileName
     * @param fileName e.g. path to file
     */
    public static LimboDB create(String url, String fileName) throws SQLException {
        return new LimboDB(url, fileName);
    }

    // TODO: receive config as argument
    private LimboDB(String url, String fileName) throws SQLException {
        super(url, fileName);
    }

    /**
     * Loads the SQLite interface backend.
     */
    public void load() {
        if (isLoaded) return;

        try {
            System.loadLibrary("_limbo_java");

        } finally {
            isLoaded = true;
        }
    }

    // WRAPPER FUNCTIONS ////////////////////////////////////////////

    // TODO: add support for JNI
    @Override
    protected synchronized native long _open_utf8(byte[] file, int openFlags) throws SQLException;

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
    protected void _open(String fileName, int openFlags) throws SQLException {
        if (isOpen) {
            throwLimboException(LimboErrorCode.UNKNOWN_ERROR.code, "Already opened");
        }
        dbPtr = _open_utf8(stringToUtf8ByteArray(fileName), openFlags);
        isOpen = true;
    }

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

    /**
     * Throws formatted SQLException with error code and message.
     *
     * @param errorCode         Error code.
     * @param errorMessageBytes Error message.
     */
    @NativeInvocation
    private void throwLimboException(int errorCode, byte[] errorMessageBytes) throws SQLException {
        String errorMessage = utf8ByteBufferToString(errorMessageBytes);
        throwLimboException(errorCode, errorMessage);
    }

    /**
     * Throws formatted SQLException with error code and message.
     *
     * @param errorCode    Error code.
     * @param errorMessage Error message.
     */
    public void throwLimboException(int errorCode, String errorMessage) throws SQLException {
        LimboErrorCode code = LimboErrorCode.getErrorCode(errorCode);
        String msg;
        if (code == LimboErrorCode.UNKNOWN_ERROR) {
            msg = String.format("%s:%s (%s)", code, errorCode, errorMessage);
        } else {
            msg = String.format("%s (%s)", code, errorMessage);
        }

        throw new LimboException(msg, code);
    }

    private static String utf8ByteBufferToString(byte[] buffer) {
        if (buffer == null) {
            return null;
        }

        return new String(buffer, StandardCharsets.UTF_8);
    }

    private static byte[] stringToUtf8ByteArray(String str) {
        if (str == null) {
            return null;
        }
        return str.getBytes(StandardCharsets.UTF_8);
    }
}
