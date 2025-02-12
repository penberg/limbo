package tech.turso.core;

import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory class for managing and creating instances of {@link LimboDB}. This class ensures that
 * multiple instances of {@link LimboDB} with the same URL are not created.
 */
public class LimboDBFactory {

  private static final ConcurrentHashMap<String, LimboDB> databaseHolder =
      new ConcurrentHashMap<>();

  /**
   * If a database with the same URL already exists, it returns the existing instance. Otherwise, it
   * creates a new instance and stores it in the database holder.
   *
   * @param url the URL of the database
   * @param filePath the path to the database file
   * @param properties additional properties for the database connection
   * @return an instance of {@link LimboDB}
   * @throws SQLException if there is an error opening the connection
   * @throws IllegalArgumentException if the fileName is empty
   */
  public static LimboDB open(String url, String filePath, Properties properties)
      throws SQLException {
    if (databaseHolder.containsKey(url)) {
      return databaseHolder.get(url);
    }

    if (filePath.isEmpty()) {
      throw new IllegalArgumentException("filePath should not be empty");
    }

    final LimboDB database;
    try {
      LimboDB.load();
      database = LimboDB.create(url, filePath);
    } catch (Exception e) {
      throw new SQLException("Error opening connection", e);
    }

    database.open(0);
    databaseHolder.put(url, database);
    return database;
  }
}
