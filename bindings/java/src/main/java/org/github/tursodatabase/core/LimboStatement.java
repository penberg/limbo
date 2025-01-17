package org.github.tursodatabase.core;

import org.github.tursodatabase.annotations.NativeInvocation;
import org.github.tursodatabase.annotations.Nullable;
import org.github.tursodatabase.jdbc4.JDBC4ResultSet;
import org.github.tursodatabase.utils.LimboExceptionUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public abstract class LimboStatement {

    protected final LimboConnection connection;
    protected final LimboResultSet resultSet;

    @Nullable
    protected String sql = null;

    protected LimboStatement(LimboConnection connection) {
        this.connection = connection;
        this.resultSet = new JDBC4ResultSet(this);
    }

    protected void internalClose() throws SQLException {
        // TODO
    }

    protected void clearGeneratedKeys() throws SQLException {
        // TODO
    }

    protected void updateGeneratedKeys() throws SQLException {
        // TODO
    }

    // TODO: enhance
    protected List<Object[]> execute(long stmtPointer) throws SQLException {
        List<Object[]> result = new ArrayList<>();
        while (true) {
            Object[] stepResult = step(stmtPointer);
            if (stepResult != null) {
                for (int i = 0; i < stepResult.length; i++) {
                    System.out.println("stepResult" + i + ": " + stepResult[i]);
                }
            }
            if (stepResult == null) break;
            result.add(stepResult);
        }

        return result;
    }

    private native Object[] step(long stmtPointer) throws SQLException;

    /**
     * Throws formatted SQLException with error code and message.
     *
     * @param errorCode         Error code.
     * @param errorMessageBytes Error message.
     */
    @NativeInvocation
    private void throwLimboException(int errorCode, byte[] errorMessageBytes) throws SQLException {
        LimboExceptionUtils.throwLimboException(errorCode, errorMessageBytes);
    }
}
