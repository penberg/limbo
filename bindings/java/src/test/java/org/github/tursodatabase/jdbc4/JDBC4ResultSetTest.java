package org.github.tursodatabase.jdbc4;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import org.github.tursodatabase.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
}
