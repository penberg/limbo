package org.github.seonwkim;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class Main {
  public static void main(String[] args) {
    try (Connection conn = DriverManager.getConnection("jdbc:sqlite:sample.db")) {
      Statement stmt =
          conn.createStatement(
              ResultSet.TYPE_FORWARD_ONLY,
              ResultSet.CONCUR_READ_ONLY,
              ResultSet.CLOSE_CURSORS_AT_COMMIT);
      stmt.execute("CREATE TABLE users (id INT PRIMARY KEY, username TEXT);");
      stmt.execute("INSERT INTO users VALUES (1, 'seonwoo');");
      stmt.execute("INSERT INTO users VALUES (2, 'seonwoo');");
      stmt.execute("INSERT INTO users VALUES (3, 'seonwoo');");
      stmt.execute("SELECT * FROM users");
      System.out.println(
          "result: " + stmt.getResultSet().getInt(1) + ", " + stmt.getResultSet().getString(2));
    } catch (Exception e) {
      System.out.println("Error: " + e);
    }
  }
}
