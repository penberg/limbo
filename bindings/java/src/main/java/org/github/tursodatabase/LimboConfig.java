package org.github.tursodatabase;

import java.util.Properties;

/**
 * Limbo Configuration.
 */
public class LimboConfig {
    private final Properties pragma;

    public LimboConfig(Properties properties) {
        this.pragma = properties;
    }

    public Properties toProperties() {
        Properties copy = new Properties();
        copy.putAll(pragma);
        return copy;
    }
}
