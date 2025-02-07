package org.github.tursodatabase.core;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;
import org.github.tursodatabase.TestUtils;
import org.github.tursodatabase.jdbc4.JDBC4Connection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LimboStatementTest {

  private JDBC4Connection connection;

  @BeforeEach
  void setUp() throws Exception {
    String filePath = TestUtils.createTempFile();
    String url = "jdbc:sqlite:" + filePath;
    connection = new JDBC4Connection(url, filePath, new Properties());
  }

  @Test
  void closing_statement_closes_related_resources() throws Exception {
    LimboStatement stmt = connection.prepare("SELECT 1;");
    stmt.execute();

    stmt.close();
    assertTrue(stmt.isClosed());
    assertFalse(stmt.getResultSet().isOpen());
  }

  @Test
  void test_initializeColumnMetadata() throws Exception {
    runSql("CREATE TABLE users (name TEXT, age INT, country TEXT);");
    runSql("INSERT INTO users VALUES ('seonwoo', 30, 'KR');");

    final LimboStatement stmt = connection.prepare("SELECT * FROM users");
    stmt.initializeColumnMetadata();
    final LimboResultSet rs = stmt.getResultSet();
    final String[] columnNames = rs.getColumnNames();

    assertEquals("name", columnNames[0]);
    assertEquals("age", columnNames[1]);
    assertEquals("country", columnNames[2]);
  }

  @Test
  void test_bindNull() throws Exception {
    runSql("CREATE TABLE test (col1 TEXT);");
    LimboStatement stmt = connection.prepare("INSERT INTO test (col1) VALUES (?);");
    stmt.bindNull(1);
    stmt.execute();
    stmt.close();

    LimboStatement selectStmt = connection.prepare("SELECT col1 FROM test;");
    selectStmt.execute();
    assertNull(selectStmt.getResultSet().get(1));
    selectStmt.close();
  }

  @Test
  void test_bindLong() throws Exception {
    runSql("CREATE TABLE test (col1 BIGINT);");
    LimboStatement stmt = connection.prepare("INSERT INTO test (col1) VALUES (?);");
    stmt.bindLong(1, 123456789L);
    stmt.execute();
    stmt.close();

    LimboStatement selectStmt = connection.prepare("SELECT col1 FROM test;");
    selectStmt.execute();
    assertEquals(123456789L, selectStmt.getResultSet().get(1));
    selectStmt.close();
  }

  @Test
  void test_bindDouble() throws Exception {
    runSql("CREATE TABLE test (col1 DOUBLE);");
    LimboStatement stmt = connection.prepare("INSERT INTO test (col1) VALUES (?);");
    stmt.bindDouble(1, 3.14);
    stmt.execute();
    stmt.close();

    LimboStatement selectStmt = connection.prepare("SELECT col1 FROM test;");
    selectStmt.execute();
    assertEquals(3.14, selectStmt.getResultSet().get(1));
    selectStmt.close();
  }

  @Test
  void test_bindText() throws Exception {
    runSql("CREATE TABLE test (col1 TEXT);");
    LimboStatement stmt = connection.prepare("INSERT INTO test (col1) VALUES (?);");
    stmt.bindText(1, "hello");
    stmt.execute();
    stmt.close();

    LimboStatement selectStmt = connection.prepare("SELECT col1 FROM test;");
    selectStmt.execute();
    assertEquals("hello", selectStmt.getResultSet().get(1));
    selectStmt.close();
  }

  @Test
  void test_bindBlob() throws Exception {
    runSql("CREATE TABLE test (col1 BLOB);");
    LimboStatement stmt = connection.prepare("INSERT INTO test (col1) VALUES (?);");
    byte[] blob = {1, 2, 3, 4, 5};
    stmt.bindBlob(1, blob);
    stmt.execute();
    stmt.close();

    LimboStatement selectStmt = connection.prepare("SELECT col1 FROM test;");
    selectStmt.execute();
    assertArrayEquals(blob, (byte[]) selectStmt.getResultSet().get(1));
    selectStmt.close();
  }

  private void runSql(String sql) throws Exception {
    LimboStatement stmt = connection.prepare(sql);
    while (stmt.execute()) {
      stmt.execute();
    }
  }
}
