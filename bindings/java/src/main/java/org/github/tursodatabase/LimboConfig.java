package org.github.tursodatabase;

import java.sql.DriverPropertyInfo;
import java.util.Arrays;
import java.util.Properties;

/** Limbo Configuration. */
public class LimboConfig {
  private final Properties pragma;

  public LimboConfig(Properties properties) {
    this.pragma = properties;
  }

  public static DriverPropertyInfo[] getDriverPropertyInfo() {
    return Arrays.stream(Pragma.values())
        .map(
            p -> {
              DriverPropertyInfo info = new DriverPropertyInfo(p.pragmaName, null);
              info.description = p.description;
              info.choices = p.choices;
              info.required = false;
              return info;
            })
        .toArray(DriverPropertyInfo[]::new);
  }

  public Properties toProperties() {
    Properties copy = new Properties();
    copy.putAll(pragma);
    return copy;
  }

  public enum Pragma {
    ;
    private final String pragmaName;
    private final String description;
    private final String[] choices;

    Pragma(String pragmaName, String description, String[] choices) {
      this.pragmaName = pragmaName;
      this.description = description;
      this.choices = choices;
    }

    public String getPragmaName() {
      return pragmaName;
    }
  }
}
