package tech.turso;

import tech.turso.core.SqliteCode;

/** Limbo error code. Superset of SQLite3 error code. */
public enum LimboErrorCode {
  SQLITE_OK(SqliteCode.SQLITE_OK, "Successful result"),
  SQLITE_ERROR(SqliteCode.SQLITE_ERROR, "SQL error or missing database"),
  SQLITE_INTERNAL(SqliteCode.SQLITE_INTERNAL, "An internal logic error in SQLite"),
  SQLITE_PERM(SqliteCode.SQLITE_PERM, "Access permission denied"),
  SQLITE_ABORT(SqliteCode.SQLITE_ABORT, "Callback routine requested an abort"),
  SQLITE_BUSY(SqliteCode.SQLITE_BUSY, "The database file is locked"),
  SQLITE_LOCKED(SqliteCode.SQLITE_LOCKED, "A table in the database is locked"),
  SQLITE_NOMEM(SqliteCode.SQLITE_NOMEM, "A malloc() failed"),
  SQLITE_READONLY(SqliteCode.SQLITE_READONLY, "Attempt to write a readonly database"),
  SQLITE_INTERRUPT(SqliteCode.SQLITE_INTERRUPT, "Operation terminated by sqlite_interrupt()"),
  SQLITE_IOERR(SqliteCode.SQLITE_IOERR, "Some kind of disk I/O error occurred"),
  SQLITE_CORRUPT(SqliteCode.SQLITE_CORRUPT, "The database disk image is malformed"),
  SQLITE_NOTFOUND(SqliteCode.SQLITE_NOTFOUND, "(Internal Only) Table or record not found"),
  SQLITE_FULL(SqliteCode.SQLITE_FULL, "Insertion failed because database is full"),
  SQLITE_CANTOPEN(SqliteCode.SQLITE_CANTOPEN, "Unable to open the database file"),
  SQLITE_PROTOCOL(SqliteCode.SQLITE_PROTOCOL, "Database lock protocol error"),
  SQLITE_EMPTY(SqliteCode.SQLITE_EMPTY, "(Internal Only) Database table is empty"),
  SQLITE_SCHEMA(SqliteCode.SQLITE_SCHEMA, "The database schema changed"),
  SQLITE_TOOBIG(SqliteCode.SQLITE_TOOBIG, "Too much data for one row of a table"),
  SQLITE_CONSTRAINT(SqliteCode.SQLITE_CONSTRAINT, "Abort due to constraint violation"),
  SQLITE_MISMATCH(SqliteCode.SQLITE_MISMATCH, "Data type mismatch"),
  SQLITE_MISUSE(SqliteCode.SQLITE_MISUSE, "Library used incorrectly"),
  SQLITE_NOLFS(SqliteCode.SQLITE_NOLFS, "Uses OS features not supported on host"),
  SQLITE_AUTH(SqliteCode.SQLITE_AUTH, "Authorization denied"),
  SQLITE_ROW(SqliteCode.SQLITE_ROW, "sqlite_step() has another row ready"),
  SQLITE_DONE(SqliteCode.SQLITE_DONE, "sqlite_step() has finished executing"),
  SQLITE_INTEGER(SqliteCode.SQLITE_INTEGER, "Integer type"),
  SQLITE_FLOAT(SqliteCode.SQLITE_FLOAT, "Float type"),
  SQLITE_TEXT(SqliteCode.SQLITE_TEXT, "Text type"),
  SQLITE_BLOB(SqliteCode.SQLITE_BLOB, "Blob type"),
  SQLITE_NULL(SqliteCode.SQLITE_NULL, "Null type"),

  UNKNOWN_ERROR(-1, "Unknown error"),
  LIMBO_FAILED_TO_PARSE_BYTE_ARRAY(1100, "Failed to parse ut8 byte array"),
  LIMBO_FAILED_TO_PREPARE_STATEMENT(1200, "Failed to prepare statement"),
  LIMBO_ETC(9999, "Unclassified error");

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
    for (LimboErrorCode limboErrorCode : LimboErrorCode.values()) {
      if (errorCode == limboErrorCode.code) return limboErrorCode;
    }

    return UNKNOWN_ERROR;
  }

  @Override
  public String toString() {
    return "LimboErrorCode{" + "code=" + code + ", message='" + message + '\'' + '}';
  }
}
