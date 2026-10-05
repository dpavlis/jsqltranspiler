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

import ai.starlake.transpiler.JSQLColumResolver;
import ai.starlake.transpiler.schema.treebuilder.JSONObjectTreeBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Read-only regressions against Snowflake's shared TPCH sample database. */
@Execution(ExecutionMode.SAME_THREAD)
class SnowflakeLiveMetaDataTest {
  private static final String DATABASE = "SNOWFLAKE_SAMPLE_DATA";
  private static final String SCHEMA = "TPCH_SF1";
  private static Connection connection;
  private static String warehouse;

  @BeforeAll
  static void connect() throws Exception {
    connection = LiveDatabaseProperties.connect("snowflake");
    try (Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SHOW WAREHOUSES")) {
      while (rs.next()) {
        String name = rs.getString("name");
        if (warehouse == null || "X-Small".equalsIgnoreCase(rs.getString("size"))) {
          warehouse = name;
        }
        if ("X-Small".equalsIgnoreCase(rs.getString("size"))) {
          break;
        }
      }
    }
    if (warehouse != null) {
      try (Statement statement = connection.createStatement()) {
        statement.execute(
            "USE WAREHOUSE " + JdbcUtils.quoteIdentifier(connection.getMetaData(), warehouse));
      }
    }
    assertEquals(DATABASE, connection.getCatalog());
    assertEquals(SCHEMA, connection.getSchema());
  }

  @AfterAll
  static void close() throws SQLException {
    if (connection != null) {
      connection.close();
    }
  }

  @Test
  void nativeSampleMetadataSupportsLineageAndJson() throws Exception {
    boolean autoCommit = connection.getAutoCommit();
    long started = System.nanoTime();
    JdbcMetaData metadata = new JdbcMetaData(connection, List.of(DATABASE + "." + SCHEMA),
        JdbcMetaDataOptions.defaults());
    java.util.logging.Logger.getLogger(getClass().getName()).info(
        "Snowflake defaults extraction took " + (System.nanoTime() - started) / 1_000_000 + " ms");
    JdbcTable region = metadata.get(DATABASE).get(SCHEMA).get("REGION");
    assertNotNull(region);
    assertEquals(3, region.columns.size());
    JdbcColumn key = region.columns.get("R_REGIONKEY");
    assertEquals(DATABASE, key.tableCatalog);
    assertEquals(SCHEMA, key.tableSchema);
    assertTrue(key.columnSize > 0);
    assertEquals(0, key.decimalDigits);
    String sql = "SELECT r.R_REGIONKEY, n.N_NAME FROM REGION r JOIN NATION n "
        + "ON n.N_REGIONKEY=r.R_REGIONKEY";
    org.json.JSONArray lineage = new JSQLColumResolver(metadata)
        .getLineage(JSONObjectTreeBuilder.class, sql).getJSONArray("columnSet");
    assertEquals(DATABASE + "." + SCHEMA + ".REGION.R_REGIONKEY",
        lineage.getJSONObject(0).getString("scope"));
    assertEquals(DATABASE + "." + SCHEMA + ".NATION.N_NAME",
        lineage.getJSONObject(1).getString("scope"));
    JdbcMetaData restored = JdbcJSONSerializer
        .fromJson(new StringReader(JdbcJSONSerializer.toJson(metadata).toString()));
    assertEquals(key.columnSize,
        restored.get(DATABASE).get(SCHEMA).get("REGION").columns.get("R_REGIONKEY").columnSize);
    JdbcColumn date = metadata.get(DATABASE).get(SCHEMA).get("ORDERS").columns.get("O_ORDERDATE");
    assertEquals(10, date.columnSize);
    assertEquals(10,
        restored.get(DATABASE).get(SCHEMA).get("ORDERS").columns.get("O_ORDERDATE").columnSize);
    if (warehouse != null) {
      try (Statement st = connection.createStatement();
          ResultSet rs = st.executeQuery("SELECT O_ORDERDATE FROM ORDERS LIMIT 0")) {
        assertEquals(rs.getMetaData().getPrecision(1), date.columnSize.intValue());
      }
    }
    assertEquals(lineage.toString(), new JSQLColumResolver(restored)
        .getLineage(JSONObjectTreeBuilder.class, sql).getJSONArray("columnSet").toString());
    assertEquals(DATABASE, connection.getCatalog());
    assertEquals(SCHEMA, connection.getSchema());
    assertEquals(autoCommit, connection.getAutoCommit());
  }

  @Test
  void discoversRequestedSampleCatalogWhenAnotherCatalogIsCurrent() throws Exception {
    String catalog = connection.getCatalog();
    String schema = connection.getSchema();
    try {
      connection.setCatalog("SNOWFLAKE");
      connection.setSchema("ACCOUNT_USAGE");
      JdbcMetaData metadata =
          new JdbcMetaData(connection, List.of("\"" + DATABASE + "\".TPCH_SF%"));
      assertNotNull(metadata.get(DATABASE).get(SCHEMA).get("REGION"));
      assertNotNull(metadata.get(DATABASE).get("TPCH_SF10").get("REGION"));
      assertEquals("SNOWFLAKE", connection.getCatalog());
      assertEquals("ACCOUNT_USAGE", connection.getSchema());
    } finally {
      connection.setCatalog(catalog);
      connection.setSchema(schema);
    }
  }

  @Test
  void explicitInformationSchemaHelperReadsSampleDatabaseAcrossCatalogs() throws Exception {
    assumeTrue(warehouse != null, "No warehouse available for INFORMATION_SCHEMA queries");
    String catalog = connection.getCatalog();
    String schema = connection.getSchema();
    try {
      connection.setCatalog("SNOWFLAKE");
      connection.setSchema("ACCOUNT_USAGE");
      List<JdbcColumn> columns = new ArrayList<>(
          JdbcTable.getColumnsFromSchemaInformation(connection, DATABASE, SCHEMA, "REGION"));
      assertEquals(3, columns.size());
      JdbcColumn key = columns.stream().filter(c -> c.columnName.equals("R_REGIONKEY")).findFirst()
          .orElseThrow();
      assertEquals(DATABASE, key.tableCatalog);
      assertEquals(SCHEMA, key.tableSchema);
      assertTrue(key.columnSize > 0);
      assertEquals(0, key.decimalDigits);
      assertEquals("SNOWFLAKE", connection.getCatalog());
      assertEquals("ACCOUNT_USAGE", connection.getSchema());
      assertTrue(JdbcTable.getColumnsFromSchemaInformation(connection, DATABASE, SCHEMA,
          "JSQLTRANSPILER_MISSING_TABLE").isEmpty());
    } finally {
      connection.setCatalog(catalog);
      connection.setSchema(schema);
    }
  }
}
