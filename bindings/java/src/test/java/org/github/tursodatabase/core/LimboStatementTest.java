package org.github.tursodatabase.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

    assertEquals(columnNames[0], "name");
    assertEquals(columnNames[1], "age");
    assertEquals(columnNames[2], "country");
  }

  private void runSql(String sql) throws Exception {
    LimboStatement stmt = connection.prepare(sql);
    while (stmt.execute()) {
      stmt.execute();
    }
  }
}
