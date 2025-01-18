package org.github.tursodatabase;

import org.github.tursodatabase.jdbc4.JDBC4Connection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class IntegrationTest {

    private JDBC4Connection connection;

    @BeforeEach
    void setUp() throws Exception {
        String filePath = TestUtils.createTempFile();
        String url = "jdbc:sqlite:" + filePath;
        connection = new JDBC4Connection(url, filePath, new Properties());
    }

    @Test
    @Disabled("Doesn't work on workflow. Need investigation.")
    void create_table_multi_inserts_select() throws Exception {
        Statement stmt = createDefaultStatement();
        stmt.execute("CREATE TABLE users (id INT PRIMARY KEY, username TEXT);");
        stmt.execute("INSERT INTO users VALUES (1, 'seonwoo');");
        stmt.execute("INSERT INTO users VALUES (2, 'seonwoo');");
        stmt.execute("INSERT INTO users VALUES (3, 'seonwoo');");
        stmt.execute("SELECT * FROM users");
    }

    private Statement createDefaultStatement() throws SQLException {
        return connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }
}
