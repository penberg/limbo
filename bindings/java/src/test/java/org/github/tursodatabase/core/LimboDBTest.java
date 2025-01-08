package org.github.tursodatabase.core;

import org.github.tursodatabase.TestUtils;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class LimboDBTest {

    @Test
    void db_should_open_normally() throws Exception {
        String dbPath = TestUtils.createTempFile();
        LimboDB db = LimboDB.create("jdbc:sqlite" + dbPath, dbPath);
        db.load();
        db.open(0);
    }

    @Test
    void should_throw_exception_when_opened_twice() throws Exception {
        String dbPath = TestUtils.createTempFile();
        LimboDB db = LimboDB.create("jdbc:sqlite:" + dbPath, dbPath);
        db.load();
        db.open(0);

        assertThatThrownBy(() -> db.open(0)).isInstanceOf(SQLException.class);
    }
}
