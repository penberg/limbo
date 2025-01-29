package org.github.tursodatabase.jdbc4;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.stream.Stream;
import org.github.tursodatabase.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class JDBC4ResultSetTest {

  private Statement stmt;

  @BeforeEach
  void setUp() throws Exception {
    String filePath = TestUtils.createTempFile();
    String url = "jdbc:sqlite:" + filePath;
    final JDBC4Connection connection = new JDBC4Connection(url, filePath, new Properties());
    stmt =
        connection.createStatement(
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.CLOSE_CURSORS_AT_COMMIT);
  }

  @Test
  void invoking_next_before_the_last_row_should_return_true() throws Exception {
    stmt.executeUpdate("CREATE TABLE users (id INT PRIMARY KEY, username TEXT);");
    stmt.executeUpdate("INSERT INTO users VALUES (1, 'sinwoo');");
    stmt.executeUpdate("INSERT INTO users VALUES (2, 'seonwoo');");

    // first call to next occur internally
    stmt.executeQuery("SELECT * FROM users");
    ResultSet resultSet = stmt.getResultSet();

    assertTrue(resultSet.next());
  }

  @Test
  void invoking_next_after_the_last_row_should_return_false() throws Exception {
    stmt.executeUpdate("CREATE TABLE users (id INT PRIMARY KEY, username TEXT);");
    stmt.executeUpdate("INSERT INTO users VALUES (1, 'sinwoo');");
    stmt.executeUpdate("INSERT INTO users VALUES (2, 'seonwoo');");

    // first call to next occur internally
    stmt.executeQuery("SELECT * FROM users");
    ResultSet resultSet = stmt.getResultSet();

    while (resultSet.next()) {
      // run until next() returns false
    }

    // if the previous call to next() returned false, consecutive call to next() should return false
    // as well
    assertFalse(resultSet.next());
  }

  @Test
  void close_resultSet_test() throws Exception {
    stmt.executeQuery("SELECT 1;");
    ResultSet resultSet = stmt.getResultSet();

    assertFalse(resultSet.isClosed());
    resultSet.close();
    assertTrue(resultSet.isClosed());
  }

  @Test
  void calling_methods_on_closed_resultSet_should_throw_exception() throws Exception {
    stmt.executeQuery("SELECT 1;");
    ResultSet resultSet = stmt.getResultSet();
    resultSet.close();
    assertTrue(resultSet.isClosed());

    assertThrows(SQLException.class, resultSet::next);
  }

  @Test
  void test_getString() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_string (string_col TEXT);");
    stmt.executeUpdate("INSERT INTO test_string (string_col) VALUES ('test');");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_string");
    assertTrue(resultSet.next());
    assertEquals("test", resultSet.getString(1));
  }

  @Test
  void test_getString_returns_null_on_null() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_null (string_col TEXT);");
    stmt.executeUpdate("INSERT INTO test_null (string_col) VALUES (NULL);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_null");
    assertTrue(resultSet.next());
    assertNull(resultSet.getString(1));
  }

  @Test
  void test_getBoolean_true() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_boolean (boolean_col INTEGER);");
    stmt.executeUpdate("INSERT INTO test_boolean (boolean_col) VALUES (1);");
    stmt.executeUpdate("INSERT INTO test_boolean (boolean_col) VALUES (2);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_boolean");

    assertTrue(resultSet.next());
    assertTrue(resultSet.getBoolean(1));

    resultSet.next();
    assertTrue(resultSet.getBoolean(1));
  }

  @Test
  void test_getBoolean_false() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_boolean (boolean_col INTEGER);");
    stmt.executeUpdate("INSERT INTO test_boolean (boolean_col) VALUES (0);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_boolean");
    assertTrue(resultSet.next());
    assertFalse(resultSet.getBoolean(1));
  }

  @Test
  void test_getBoolean_returns_false_on_null() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_null (boolean_col INTEGER);");
    stmt.executeUpdate("INSERT INTO test_null (boolean_col) VALUES (NULL);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_null");
    assertTrue(resultSet.next());
    assertFalse(resultSet.getBoolean(1));
  }

  @Test
  void test_getByte() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_byte (byte_col INTEGER);");
    stmt.executeUpdate("INSERT INTO test_byte (byte_col) VALUES (1);");
    stmt.executeUpdate("INSERT INTO test_byte (byte_col) VALUES (128);"); // Exceeds byte size
    stmt.executeUpdate("INSERT INTO test_byte (byte_col) VALUES (-129);"); // Exceeds byte size

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_byte");

    // Test value that fits within byte size
    assertTrue(resultSet.next());
    assertEquals(1, resultSet.getByte(1));

    // Test value that exceeds byte size (positive overflow)
    assertTrue(resultSet.next());
    assertEquals(-128, resultSet.getByte(1)); // 128 overflows to -128

    // Test value that exceeds byte size (negative overflow)
    assertTrue(resultSet.next());
    assertEquals(127, resultSet.getByte(1)); // -129 overflows to 127
  }

  @Test
  void test_getByte_returns_zero_on_null() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_null (byte_col INTEGER);");
    stmt.executeUpdate("INSERT INTO test_null (byte_col) VALUES (NULL);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_null");
    assertTrue(resultSet.next());
    assertEquals(0, resultSet.getByte(1));
  }

  @Test
  void test_getShort() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_short (short_col INTEGER);");
    stmt.executeUpdate("INSERT INTO test_short (short_col) VALUES (123);");
    stmt.executeUpdate("INSERT INTO test_short (short_col) VALUES (32767);"); // Max short value
    stmt.executeUpdate("INSERT INTO test_short (short_col) VALUES (-32768);"); // Min short value

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_short");

    // Test typical short value
    assertTrue(resultSet.next());
    assertEquals(123, resultSet.getShort(1));

    // Test maximum short value
    assertTrue(resultSet.next());
    assertEquals(32767, resultSet.getShort(1));

    // Test minimum short value
    assertTrue(resultSet.next());
    assertEquals(-32768, resultSet.getShort(1));
  }

  @Test
  void test_getShort_returns_zero_on_null() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_null (short_col INTEGER);");
    stmt.executeUpdate("INSERT INTO test_null (short_col) VALUES (NULL);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_null");
    assertTrue(resultSet.next());
    assertEquals(0, resultSet.getShort(1));
  }

  @Test
  void test_getInt() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_int (int_col INT);");
    stmt.executeUpdate("INSERT INTO test_int (int_col) VALUES (12345);");
    stmt.executeUpdate("INSERT INTO test_int (int_col) VALUES (2147483647);"); // Max int value
    stmt.executeUpdate("INSERT INTO test_int (int_col) VALUES (-2147483648);"); // Min int value

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_int");

    // Test typical int value
    assertTrue(resultSet.next());
    assertEquals(12345, resultSet.getInt(1));

    // Test maximum int value
    assertTrue(resultSet.next());
    assertEquals(2147483647, resultSet.getInt(1));

    // Test minimum int value
    assertTrue(resultSet.next());
    assertEquals(-2147483648, resultSet.getInt(1));
  }

  @Test
  void test_getInt_returns_zero_on_null() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_null (int_col INTEGER);");
    stmt.executeUpdate("INSERT INTO test_null (int_col) VALUES (NULL);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_null");
    assertTrue(resultSet.next());
    assertEquals(0, resultSet.getInt(1));
  }

  @Test
  @Disabled("limbo has a bug which sees -9223372036854775808 as double")
  void test_getLong() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_long (long_col BIGINT);");
    stmt.executeUpdate("INSERT INTO test_long (long_col) VALUES (1234567890);");
    stmt.executeUpdate(
        "INSERT INTO test_long (long_col) VALUES (9223372036854775807);"); // Max long value
    stmt.executeUpdate(
        "INSERT INTO test_long (long_col) VALUES (-9223372036854775808);"); // Min long value

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_long");

    // Test typical long value
    assertEquals(1234567890L, resultSet.getLong(1));

    // Test maximum long value
    assertTrue(resultSet.next());
    assertEquals(9223372036854775807L, resultSet.getLong(1));

    // Test minimum long value
    assertTrue(resultSet.next());
    assertEquals(-9223372036854775808L, resultSet.getLong(1));
  }

  @Test
  void test_getLong_returns_zero_no_null() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_null (long_col INTEGER);");
    stmt.executeUpdate("INSERT INTO test_null (long_col) VALUES (NULL);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_null");
    assertTrue(resultSet.next());
    assertEquals(0L, resultSet.getLong(1));
  }

  @Test
  void test_getFloat() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_float (float_col REAL);");
    stmt.executeUpdate("INSERT INTO test_float (float_col) VALUES (1.23);");
    stmt.executeUpdate(
        "INSERT INTO test_float (float_col) VALUES (3.4028235E38);"); // Max float value
    stmt.executeUpdate(
        "INSERT INTO test_float (float_col) VALUES (1.4E-45);"); // Min positive float value
    stmt.executeUpdate(
        "INSERT INTO test_float (float_col) VALUES (-3.4028235E38);"); // Min negative float value

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_float");

    // Test typical float value
    assertTrue(resultSet.next());
    assertEquals(1.23f, resultSet.getFloat(1), 0.0001);

    // Test maximum float value
    assertTrue(resultSet.next());
    assertEquals(3.4028235E38f, resultSet.getFloat(1), 0.0001);

    // Test minimum positive float value
    assertTrue(resultSet.next());
    assertEquals(1.4E-45f, resultSet.getFloat(1), 0.0001);

    // Test minimum negative float value
    assertTrue(resultSet.next());
    assertEquals(-3.4028235E38f, resultSet.getFloat(1), 0.0001);
  }

  @Test
  void test_getFloat_returns_zero_on_null() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_null (float_col REAL);");
    stmt.executeUpdate("INSERT INTO test_null (float_col) VALUES (NULL);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_null");
    assertTrue(resultSet.next());
    assertEquals(0.0f, resultSet.getFloat(1), 0.0001);
  }

  @Test
  void test_getDouble() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_double (double_col REAL);");
    stmt.executeUpdate("INSERT INTO test_double (double_col) VALUES (1.234567);");
    stmt.executeUpdate(
        "INSERT INTO test_double (double_col) VALUES (1.7976931348623157E308);"); // Max double
    // value
    stmt.executeUpdate(
        "INSERT INTO test_double (double_col) VALUES (4.9E-324);"); // Min positive double value
    stmt.executeUpdate(
        "INSERT INTO test_double (double_col) VALUES (-1.7976931348623157E308);"); // Min negative
    // double value

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_double");

    // Test typical double value
    assertTrue(resultSet.next());
    assertEquals(1.234567, resultSet.getDouble(1), 0.0001);

    // Test maximum double value
    assertTrue(resultSet.next());
    assertEquals(1.7976931348623157E308, resultSet.getDouble(1), 0.0001);

    // Test minimum positive double value
    assertTrue(resultSet.next());
    assertEquals(4.9E-324, resultSet.getDouble(1), 0.0001);

    // Test minimum negative double value
    assertTrue(resultSet.next());
    assertEquals(-1.7976931348623157E308, resultSet.getDouble(1), 0.0001);
  }

  @Test
  void test_getDouble_returns_zero_on_null() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_null (double_col REAL);");
    stmt.executeUpdate("INSERT INTO test_null (double_col) VALUES (NULL);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_null");
    assertTrue(resultSet.next());
    assertEquals(0.0, resultSet.getDouble(1), 0.0001);
  }

  @Test
  void test_getBigDecimal() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_bigdecimal (bigdecimal_col REAL);");
    stmt.executeUpdate("INSERT INTO test_bigdecimal (bigdecimal_col) VALUES (12345.67);");
    stmt.executeUpdate(
        "INSERT INTO test_bigdecimal (bigdecimal_col) VALUES (1.7976931348623157E308);"); // Max
    // double
    // value
    stmt.executeUpdate(
        "INSERT INTO test_bigdecimal (bigdecimal_col) VALUES (4.9E-324);"); // Min positive double
    // value
    stmt.executeUpdate(
        "INSERT INTO test_bigdecimal (bigdecimal_col) VALUES (-12345.67);"); // Negative value

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_bigdecimal");

    // Test typical BigDecimal value
    assertTrue(resultSet.next());
    assertEquals(
        new BigDecimal("12345.67").setScale(2, RoundingMode.HALF_UP),
        resultSet.getBigDecimal(1, 2));

    // Test maximum double value
    assertTrue(resultSet.next());
    assertEquals(
        new BigDecimal("1.7976931348623157E308").setScale(10, RoundingMode.HALF_UP),
        resultSet.getBigDecimal(1, 10));

    // Test minimum positive double value
    assertTrue(resultSet.next());
    assertEquals(
        new BigDecimal("4.9E-324").setScale(10, RoundingMode.HALF_UP),
        resultSet.getBigDecimal(1, 10));

    // Test negative BigDecimal value
    assertTrue(resultSet.next());
    assertEquals(
        new BigDecimal("-12345.67").setScale(2, RoundingMode.HALF_UP),
        resultSet.getBigDecimal(1, 2));
  }

  @Test
  void test_getBigDecimal_returns_null_on_null() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_null (bigdecimal_col REAL);");
    stmt.executeUpdate("INSERT INTO test_null (bigdecimal_col) VALUES (NULL);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_null");
    assertTrue(resultSet.next());
    assertNull(resultSet.getBigDecimal(1, 2));
  }

  @ParameterizedTest
  @MethodSource("byteArrayProvider")
  void test_getBytes(byte[] data) throws Exception {
    stmt.executeUpdate("CREATE TABLE test_bytes (bytes_col BLOB);");
    executeDMLAndAssert(data);
  }

  private static Stream<byte[]> byteArrayProvider() {
    return Stream.of(
        "Hello".getBytes(), "world".getBytes(), new byte[0], new byte[] {0x00, (byte) 0xFF});
  }

  private void executeDMLAndAssert(byte[] data) throws SQLException {
    // Convert byte array to hexadecimal string
    StringBuilder hexString = new StringBuilder();
    for (byte b : data) {
      hexString.append(String.format("%02X", b));
    }
    // Execute DML statement
    stmt.executeUpdate("INSERT INTO test_bytes (bytes_col) VALUES (X'" + hexString + "');");

    // Assert the inserted data
    ResultSet resultSet = stmt.executeQuery("SELECT bytes_col FROM test_bytes");
    assertTrue(resultSet.next());
    assertArrayEquals(data, resultSet.getBytes(1));
  }

  @Test
  void test_getBytes_returns_null_on_null() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_null (bytes_col BLOB);");
    stmt.executeUpdate("INSERT INTO test_null (bytes_col) VALUES (NULL);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_null");
    assertTrue(resultSet.next());
    assertNull(resultSet.getBytes(1));
  }

  @Test
  void test_getXXX_methods_on_multiple_columns() throws Exception {
    stmt.executeUpdate(
        "CREATE TABLE test_integration ("
            + "string_col TEXT, "
            + "boolean_col INTEGER, "
            + "byte_col INTEGER, "
            + "short_col INTEGER, "
            + "int_col INTEGER, "
            + "long_col BIGINT, "
            + "float_col REAL, "
            + "double_col REAL, "
            + "bigdecimal_col REAL, "
            + "bytes_col BLOB);");

    stmt.executeUpdate(
        "INSERT INTO test_integration VALUES ("
            + "'test', "
            + "1, "
            + "1, "
            + "123, "
            + "12345, "
            + "1234567890, "
            + "1.23, "
            + "1.234567, "
            + "12345.67, "
            + "X'48656C6C6F');");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_integration");
    assertTrue(resultSet.next());

    // Verify each column
    assertEquals("test", resultSet.getString(1));
    assertTrue(resultSet.getBoolean(2));
    assertEquals(1, resultSet.getByte(3));
    assertEquals(123, resultSet.getShort(4));
    assertEquals(12345, resultSet.getInt(5));
    assertEquals(1234567890L, resultSet.getLong(6));
    assertEquals(1.23f, resultSet.getFloat(7), 0.0001);
    assertEquals(1.234567, resultSet.getDouble(8), 0.0001);
    assertEquals(
        new BigDecimal("12345.67").setScale(2, RoundingMode.HALF_UP),
        resultSet.getBigDecimal(9, 2));
    assertArrayEquals("Hello".getBytes(), resultSet.getBytes(10));
  }

  @Test
  void test_invalidColumnIndex_outOfBounds() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_invalid (col INTEGER);");
    stmt.executeUpdate("INSERT INTO test_invalid (col) VALUES (1);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_invalid");
    assertTrue(resultSet.next());

    // Test out-of-bounds column index
    assertThrows(SQLException.class, () -> resultSet.getInt(2));
  }

  @Test
  void test_invalidColumnIndex_negative() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_invalid (col INTEGER);");
    stmt.executeUpdate("INSERT INTO test_invalid (col) VALUES (1);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_invalid");
    assertTrue(resultSet.next());

    // Test negative column index
    assertThrows(SQLException.class, () -> resultSet.getInt(-1));
  }
}
