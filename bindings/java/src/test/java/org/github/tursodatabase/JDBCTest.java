package org.github.tursodatabase;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class JDBCTest {

    @Test
    void null_is_returned_when_invalid_url_is_passed() throws Exception {
        LimboConnection connection = JDBC.createConnection("jdbc:invalid:xxx", new Properties());
        assertThat(connection).isNull();
    }

    @Test
    void non_null_connection_is_returned_when_valid_url_is_passed() throws Exception {
        String fileUrl = TestUtils.createTempFile();
        LimboConnection connection = JDBC.createConnection("jdbc:limbo:" + fileUrl, new Properties());
        assertThat(connection).isNotNull();
    }
}
