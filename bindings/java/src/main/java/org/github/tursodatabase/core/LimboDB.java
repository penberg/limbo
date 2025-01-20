package org.github.tursodatabase.core;


import org.github.tursodatabase.LimboErrorCode;
import org.github.tursodatabase.annotations.NativeInvocation;
import org.github.tursodatabase.annotations.VisibleForTesting;
import org.github.tursodatabase.utils.LimboExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.locks.ReentrantLock;

import static org.github.tursodatabase.utils.ByteArrayUtils.stringToUtf8ByteArray;

/**
 * This class provides a thin JNI layer over the SQLite3 C API.
 */
public final class LimboDB extends AbstractDB {
    private static final Logger logger = LoggerFactory.getLogger(LimboDB.class);
    // Pointer to database instance
    private long dbPointer;
    private boolean isOpen;

    private static boolean isLoaded;
    private ReentrantLock dbLock = new ReentrantLock();

    static {
        if ("The Android Project".equals(System.getProperty("java.vm.vendor"))) {
            // TODO
        } else {
            // continue with non Android execution path
            isLoaded = false;
        }
    }

    /**
     * Loads the SQLite interface backend.
     */
    public static void load() {
        if (isLoaded) return;

        try {
            System.loadLibrary("_limbo_java");
        } finally {
            isLoaded = true;
        }
    }

    /**
     * @param url      e.g. "jdbc:sqlite:fileName
     * @param filePath e.g. path to file
     */
    public static LimboDB create(String url, String filePath) throws SQLException {
        return new LimboDB(url, filePath);
    }

    // TODO: receive config as argument
    private LimboDB(String url, String filePath) {
        super(url, filePath);
    }

    // WRAPPER FUNCTIONS ////////////////////////////////////////////

    // TODO: add support for JNI
    @Override
    protected native long openUtf8(byte[] file, int openFlags) throws SQLException;

    // TODO: add support for JNI
    @Override
    protected native void close0() throws SQLException;

    // TODO: add support for JNI
    native int execUtf8(byte[] sqlUtf8) throws SQLException;

    // TODO: add support for JNI
    @Override
    public native void interrupt();

    @Override
    protected void open0(String filePath, int openFlags) throws SQLException {
        if (isOpen) {
            throw LimboExceptionUtils.buildLimboException(LimboErrorCode.LIMBO_ETC.code, "Already opened");
        }

        byte[] filePathBytes = stringToUtf8ByteArray(filePath);
        if (filePathBytes == null) {
            throw LimboExceptionUtils.buildLimboException(LimboErrorCode.LIMBO_ETC.code, "File path cannot be converted to byteArray. File name: " + filePath);
        }

        dbPointer = openUtf8(filePathBytes, openFlags);
        isOpen = true;
    }

    @Override
    public long connect() throws SQLException {
        byte[] filePathBytes = stringToUtf8ByteArray(filePath);
        if (filePathBytes == null) {
            throw LimboExceptionUtils.buildLimboException(LimboErrorCode.LIMBO_ETC.code, "File path cannot be converted to byteArray. File name: " + filePath);
        }
        return connect0(filePathBytes, dbPointer);
    }

    private native long connect0(byte[] path, long databasePtr) throws SQLException;

    @VisibleForTesting
    native void throwJavaException(int errorCode) throws SQLException;

    /**
     * Throws formatted SQLException with error code and message.
     *
     * @param errorCode         Error code.
     * @param errorMessageBytes Error message.
     */
    @NativeInvocation(invokedFrom = "limbo_db.rs")
    private void throwLimboException(int errorCode, byte[] errorMessageBytes) throws SQLException {
        LimboExceptionUtils.throwLimboException(errorCode, errorMessageBytes);
    }
}
