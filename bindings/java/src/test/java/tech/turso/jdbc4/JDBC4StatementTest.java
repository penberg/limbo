package tech.turso.jdbc4;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import tech.turso.TestUtils;

class JDBC4StatementTest {

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
  void execute_ddl_should_return_false() throws Exception {
    assertFalse(stmt.execute("CREATE TABLE users (id INT PRIMARY KEY, username TEXT);"));
  }

  @Test
  void execute_insert_should_return_false() throws Exception {
    stmt.execute("CREATE TABLE users (id INT PRIMARY KEY, username TEXT);");
    assertFalse(stmt.execute("INSERT INTO users VALUES (1, 'limbo');"));
  }

  @Test
  @Disabled("UPDATE not supported yet")
  void execute_update_should_return_false() throws Exception {
    stmt.execute("CREATE TABLE users (id INT PRIMARY KEY, username TEXT);");
    stmt.execute("INSERT INTO users VALUES (1, 'limbo');");
    assertFalse(stmt.execute("UPDATE users SET username = 'seonwoo' WHERE id = 1;"));
  }

  @Test
  void execute_select_should_return_true() throws Exception {
    stmt.execute("CREATE TABLE users (id INT PRIMARY KEY, username TEXT);");
    stmt.execute("INSERT INTO users VALUES (1, 'limbo');");
    assertTrue(stmt.execute("SELECT * FROM users;"));
  }

  @Test
  void close_statement_test() throws Exception {
    stmt.close();
    assertTrue(stmt.isClosed());
  }

  @Test
  void double_close_is_no_op() throws SQLException {
    stmt.close();
    assertDoesNotThrow(() -> stmt.close());
  }

  @Test
  void operations_on_closed_statement_should_throw_exception() throws Exception {
    stmt.close();
    assertThrows(SQLException.class, () -> stmt.execute("SELECT 1;"));
  }
}
