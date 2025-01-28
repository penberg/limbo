package org.github.tursodatabase.core;

import static org.junit.jupiter.api.Assertions.*;

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
}
