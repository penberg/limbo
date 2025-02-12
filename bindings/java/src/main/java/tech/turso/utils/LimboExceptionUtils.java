package tech.turso.utils;

import static tech.turso.utils.ByteArrayUtils.utf8ByteBufferToString;

import java.sql.SQLException;
import tech.turso.LimboErrorCode;
import tech.turso.annotations.Nullable;
import tech.turso.exceptions.LimboException;

public class LimboExceptionUtils {
  /**
   * Throws formatted SQLException with error code and message.
   *
   * @param errorCode Error code.
   * @param errorMessageBytes Error message.
   */
  public static void throwLimboException(int errorCode, byte[] errorMessageBytes)
      throws SQLException {
    String errorMessage = utf8ByteBufferToString(errorMessageBytes);
    throw buildLimboException(errorCode, errorMessage);
  }

  /**
   * Throws formatted SQLException with error code and message.
   *
   * @param errorCode Error code.
   * @param errorMessage Error message.
   */
  public static LimboException buildLimboException(int errorCode, @Nullable String errorMessage)
      throws SQLException {
    LimboErrorCode code = LimboErrorCode.getErrorCode(errorCode);
    String msg;
    if (code == LimboErrorCode.UNKNOWN_ERROR) {
      msg = String.format("%s:%s (%s)", code, errorCode, errorMessage);
    } else {
      msg = String.format("%s (%s)", code, errorMessage);
    }

    return new LimboException(msg, code);
  }
}
