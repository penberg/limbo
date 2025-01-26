package org.github.tursodatabase;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.github.tursodatabase.core.LimboConnection;
import org.junit.jupiter.api.Test;

class JDBCTest {

  @Test
  void null_is_returned_when_invalid_url_is_passed() throws Exception {
    LimboConnection connection = JDBC.createConnection("jdbc:invalid:xxx", new Properties());
    assertThat(connection).isNull();
  }

  @Test
  void non_null_connection_is_returned_when_valid_url_is_passed() throws Exception {
    String fileUrl = TestUtils.createTempFile();
    LimboConnection connection = JDBC.createConnection("jdbc:sqlite:" + fileUrl, new Properties());
    assertThat(connection).isNotNull();
  }

  @Test
  void connection_can_be_retrieved_from_DriverManager() throws SQLException {
    try (Connection connection = DriverManager.getConnection("jdbc:sqlite:sample.db")) {
      assertThat(connection).isNotNull();
    }
  }
}
