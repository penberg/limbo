package org.github.tursodatabase.jdbc4;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Properties;

import org.github.tursodatabase.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JDBC4DatabaseMetaDataTest {

    private JDBC4Connection connection;
    private JDBC4DatabaseMetaData metaData;

    @BeforeEach
    void setUp() throws Exception {
        String filePath = TestUtils.createTempFile();
        String url = "jdbc:sqlite:" + filePath;
        connection = new JDBC4Connection(url, filePath, new Properties());
        metaData = new JDBC4DatabaseMetaData(connection);
    }

    @Test
    void getURLShouldReturnNonEmptyString() throws Exception{
        assertFalse(metaData.getURL().isEmpty());
    }
}
