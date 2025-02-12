package tech.turso.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.Properties;
import org.junit.jupiter.api.Test;
import tech.turso.TestUtils;

class LimboDBFactoryTest {

  @Test
  void single_database_should_be_created_when_urls_are_same() throws Exception {
    String filePath = TestUtils.createTempFile();
    String url = "jdbc:sqlite:" + filePath;
    LimboDB db1 = LimboDBFactory.open(url, filePath, new Properties());
    LimboDB db2 = LimboDBFactory.open(url, filePath, new Properties());
    assertEquals(db1, db2);
  }

  @Test
  void multiple_databases_should_be_created_when_urls_differ() throws Exception {
    String filePath1 = TestUtils.createTempFile();
    String filePath2 = TestUtils.createTempFile();
    String url1 = "jdbc:sqlite:" + filePath1;
    String url2 = "jdbc:sqlite:" + filePath2;
    LimboDB db1 = LimboDBFactory.open(url1, filePath1, new Properties());
    LimboDB db2 = LimboDBFactory.open(url2, filePath2, new Properties());
    assertNotEquals(db1, db2);
  }
}
