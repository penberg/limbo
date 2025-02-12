package tech.turso.exceptions;

import java.sql.SQLException;
import tech.turso.LimboErrorCode;

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
