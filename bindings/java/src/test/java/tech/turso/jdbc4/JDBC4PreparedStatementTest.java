package tech.turso.jdbc4;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.turso.TestUtils;

class JDBC4PreparedStatementTest {

  private JDBC4Connection connection;

  @BeforeEach
  void setUp() throws Exception {
    String filePath = TestUtils.createTempFile();
    String url = "jdbc:sqlite:" + filePath;
    connection = new JDBC4Connection(url, filePath, new Properties());
  }

  @Test
  void testSetBoolean() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col INTEGER)").execute();
    PreparedStatement stmt =
        connection.prepareStatement("INSERT INTO test (col) VALUES (?), (?), (?)");
    stmt.setBoolean(1, true);
    stmt.setBoolean(2, false);
    stmt.setBoolean(3, true);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT * FROM test;");
    ResultSet rs = stmt2.executeQuery();
    assertTrue(rs.next());
    assertTrue(rs.getBoolean(1));
    assertTrue(rs.next());
    assertFalse(rs.getBoolean(1));
    assertTrue(rs.next());
    assertTrue(rs.getBoolean(1));
  }

  @Test
  void testSetByte() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col INTEGER)").execute();
    PreparedStatement stmt =
        connection.prepareStatement("INSERT INTO test (col) VALUES (?), (?), (?)");
    stmt.setByte(1, (byte) 1);
    stmt.setByte(2, (byte) 2);
    stmt.setByte(3, (byte) 3);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT * FROM test;");
    ResultSet rs = stmt2.executeQuery();
    assertTrue(rs.next());
    assertEquals(1, rs.getByte(1));
    assertTrue(rs.next());
    assertEquals(2, rs.getByte(1));
    assertTrue(rs.next());
    assertEquals(3, rs.getByte(1));
  }

  @Test
  void testSetShort() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col INTEGER)").execute();
    PreparedStatement stmt =
        connection.prepareStatement("INSERT INTO test (col) VALUES (?), (?), (?)");
    stmt.setShort(1, (short) 1);
    stmt.setShort(2, (short) 2);
    stmt.setShort(3, (short) 3);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT * FROM test;");
    ResultSet rs = stmt2.executeQuery();
    assertTrue(rs.next());
    assertEquals(1, rs.getShort(1));
    assertTrue(rs.next());
    assertEquals(2, rs.getShort(1));
    assertTrue(rs.next());
    assertEquals(3, rs.getShort(1));
  }

  @Test
  void testSetInt() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col INTEGER)").execute();
    PreparedStatement stmt =
        connection.prepareStatement("INSERT INTO test (col) VALUES (?), (?), (?)");
    stmt.setInt(1, 1);
    stmt.setInt(2, 2);
    stmt.setInt(3, 3);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT * FROM test;");
    ResultSet rs = stmt2.executeQuery();
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertTrue(rs.next());
    assertEquals(3, rs.getInt(1));
  }

  @Test
  void testSetLong() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col INTEGER)").execute();
    PreparedStatement stmt =
        connection.prepareStatement("INSERT INTO test (col) VALUES (?), (?), (?)");
    stmt.setLong(1, 1L);
    stmt.setLong(2, 2L);
    stmt.setLong(3, 3L);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT * FROM test;");
    ResultSet rs = stmt2.executeQuery();
    assertTrue(rs.next());
    assertEquals(1L, rs.getLong(1));
    assertTrue(rs.next());
    assertEquals(2L, rs.getLong(1));
    assertTrue(rs.next());
    assertEquals(3L, rs.getLong(1));
  }

  @Test
  void testSetFloat() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col REAL)").execute();
    PreparedStatement stmt =
        connection.prepareStatement("INSERT INTO test (col) VALUES (?), (?), (?)");
    stmt.setFloat(1, 1.0f);
    stmt.setFloat(2, 2.0f);
    stmt.setFloat(3, 3.0f);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT * FROM test;");
    ResultSet rs = stmt2.executeQuery();
    assertTrue(rs.next());
    assertEquals(1.0f, rs.getFloat(1));
    assertTrue(rs.next());
    assertEquals(2.0f, rs.getFloat(1));
    assertTrue(rs.next());
    assertEquals(3.0f, rs.getFloat(1));
  }

  @Test
  void testSetDouble() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col REAL)").execute();
    PreparedStatement stmt =
        connection.prepareStatement("INSERT INTO test (col) VALUES (?), (?), (?)");
    stmt.setDouble(1, 1.0);
    stmt.setDouble(2, 2.0);
    stmt.setDouble(3, 3.0);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT * FROM test;");
    ResultSet rs = stmt2.executeQuery();
    assertTrue(rs.next());
    assertEquals(1.0, rs.getDouble(1));
    assertTrue(rs.next());
    assertEquals(2.0, rs.getDouble(1));
    assertTrue(rs.next());
    assertEquals(3.0, rs.getDouble(1));
  }

  @Test
  void testSetBigDecimal() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();
    PreparedStatement stmt =
        connection.prepareStatement("INSERT INTO test (col) VALUES (?), (?), (?)");
    stmt.setBigDecimal(1, new BigDecimal("1.0"));
    stmt.setBigDecimal(2, new BigDecimal("2.0"));
    stmt.setBigDecimal(3, new BigDecimal("3.0"));
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT * FROM test;");
    ResultSet rs = stmt2.executeQuery();
    assertTrue(rs.next());
    assertEquals("1.0", rs.getString(1));
    assertTrue(rs.next());
    assertEquals("2.0", rs.getString(1));
    assertTrue(rs.next());
    assertEquals("3.0", rs.getString(1));
  }

  @Test
  void testSetString() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();
    PreparedStatement stmt =
        connection.prepareStatement("INSERT INTO test (col) VALUES (?), (?), (?)");
    stmt.setString(1, "test1");
    stmt.setString(2, "test2");
    stmt.setString(3, "test3");
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT * FROM test;");
    ResultSet rs = stmt2.executeQuery();
    assertTrue(rs.next());
    assertEquals("test1", rs.getString(1));
    assertTrue(rs.next());
    assertEquals("test2", rs.getString(1));
    assertTrue(rs.next());
    assertEquals("test3", rs.getString(1));
  }

  @Test
  void testSetBytes() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col BLOB)").execute();
    PreparedStatement stmt =
        connection.prepareStatement("INSERT INTO test (col) VALUES (?), (?), (?)");
    stmt.setBytes(1, new byte[] {1, 2, 3});
    stmt.setBytes(2, new byte[] {4, 5, 6});
    stmt.setBytes(3, new byte[] {7, 8, 9});
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT * FROM test;");
    ResultSet rs = stmt2.executeQuery();
    assertTrue(rs.next());
    assertArrayEquals(new byte[] {1, 2, 3}, rs.getBytes(1));
    assertTrue(rs.next());
    assertArrayEquals(new byte[] {4, 5, 6}, rs.getBytes(1));
    assertTrue(rs.next());
    assertArrayEquals(new byte[] {7, 8, 9}, rs.getBytes(1));
  }

  @Test
  void testInsertMultipleTypes() throws SQLException {
    connection
        .prepareStatement("CREATE TABLE test (col1 INTEGER, col2 REAL, col3 TEXT, col4 BLOB)")
        .execute();
    PreparedStatement stmt =
        connection.prepareStatement(
            "INSERT INTO test (col1, col2, col3, col4) VALUES (?, ?, ?, ?), (?, ?, ?, ?)");

    stmt.setInt(1, 1);
    stmt.setFloat(2, 1.1f);
    stmt.setString(3, "row1");
    stmt.setBytes(4, new byte[] {1, 2, 3});

    stmt.setInt(5, 2);
    stmt.setFloat(6, 2.2f);
    stmt.setString(7, "row2");
    stmt.setBytes(8, new byte[] {4, 5, 6});

    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT * FROM test;");
    ResultSet rs = stmt2.executeQuery();

    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals(1.1f, rs.getFloat(2));
    assertEquals("row1", rs.getString(3));
    assertArrayEquals(new byte[] {1, 2, 3}, rs.getBytes(4));

    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertEquals(2.2f, rs.getFloat(2));
    assertEquals("row2", rs.getString(3));
    assertArrayEquals(new byte[] {4, 5, 6}, rs.getBytes(4));
  }
}
