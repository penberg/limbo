package org.github.tursodatabase.exceptions;

import org.github.tursodatabase.LimboErrorCode;

import java.sql.SQLException;

public class LimboException extends SQLException {
    private final LimboErrorCode resultCode;

    public LimboException(String message, LimboErrorCode resultCode) {
        super(message, null, resultCode.code & 0xff);
        this.resultCode = resultCode;
    }

    public LimboErrorCode getResultCode() {
        return resultCode;
    }
}
