package org.github.tursodatabase.exceptions;

import java.sql.SQLException;
import org.github.tursodatabase.LimboErrorCode;

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
