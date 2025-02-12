package tech.turso.jdbc4;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.turso.TestUtils;

class JDBC4DatabaseMetaDataTest {

  private JDBC4Connection connection;
  private JDBC4DatabaseMetaData metaData;

  @BeforeEach
  void set_up() throws Exception {
    String filePath = TestUtils.createTempFile();
    String url = "jdbc:sqlite:" + filePath;
    connection = new JDBC4Connection(url, filePath, new Properties());
    metaData = new JDBC4DatabaseMetaData(connection);
  }

  @Test
  void getURL_should_return_non_empty_string() {
    assertFalse(metaData.getURL().isEmpty());
  }

  @Test
  void getDriverName_should_not_return_empty_string() {
    assertFalse(metaData.getDriverName().isEmpty());
  }

  @Test
  void test_getDriverMajorVersion() {
    metaData.getDriverMajorVersion();
  }

  @Test
  void test_getDriverMinorVersion() {
    metaData.getDriverMinorVersion();
  }

  @Test
  void getDriverVersion_should_not_return_empty_string() {
    assertFalse(metaData.getDriverVersion().isEmpty());
  }
}
