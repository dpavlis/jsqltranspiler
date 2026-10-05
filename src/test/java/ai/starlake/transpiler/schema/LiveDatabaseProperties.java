/**
 * Starlake.AI JSQLTranspiler is a SQL to DuckDB Transpiler.
 * Copyright (C) 2025 Starlake.AI (hayssam.saleh@starlake.ai)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.starlake.transpiler.schema;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Secrets are read only from the local, ignored properties file. */
final class LiveDatabaseProperties {
  private LiveDatabaseProperties() {}

  static Connection connect(String database) throws IOException, SQLException {
    return connect(database, new Properties());
  }

  static Connection connect(String database, Properties overrides)
      throws IOException, SQLException {
    Path path = Path.of(System.getProperty("liveDatabasesFile", "live-databases.properties"));
    assumeTrue(Files.isRegularFile(path), "Live database properties file is absent");
    Properties properties = new Properties();
    try (Reader reader = Files.newBufferedReader(path)) {
      properties.load(reader);
    }
    String url = properties.getProperty(database + ".url");
    assumeTrue(url != null && !url.isBlank(), "Live database endpoint is not configured");
    Properties connectionProperties = new Properties();
    connectionProperties.setProperty("user", properties.getProperty(database + ".user", ""));
    connectionProperties.setProperty("password",
        properties.getProperty(database + ".password", ""));
    connectionProperties.putAll(overrides);
    return DriverManager.getConnection(url, connectionProperties);
  }
}
