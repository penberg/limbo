package org.github.tursodatabase;

public enum LimboErrorCode {
    UNKNOWN_ERROR(-1, "Unknown error"),
    ETC(9999, "Unclassified error");

    public final int code;
    public final String message;

    /**
     * @param code Error code
     * @param message Message for the error.
     */
    LimboErrorCode(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public static LimboErrorCode getErrorCode(int errorCode) {
        for (LimboErrorCode limboErrorCode: LimboErrorCode.values()) {
            if (errorCode == limboErrorCode.code) return limboErrorCode;
        }

        return UNKNOWN_ERROR;
    }

    @Override
    public String toString() {
        return "LimboErrorCode{" +
                "code=" + code +
                ", message='" + message + '\'' +
                '}';
    }
}
