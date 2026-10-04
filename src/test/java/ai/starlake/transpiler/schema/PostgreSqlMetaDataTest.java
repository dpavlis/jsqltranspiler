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
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Live PostgreSQL regression tests; credentials and endpoint are supplied by the environment. */
@EnabledIfEnvironmentVariable(named = "POSTGRES_JDBC_URL", matches = ".+")
class PostgreSqlMetaDataTest {
  @Test
  void northwindExpressionLineage() throws Exception {
    try (Connection conn = DriverManager.getConnection(System.getenv("POSTGRES_JDBC_URL"),
        System.getenv("POSTGRES_USER"), System.getenv("POSTGRES_PASSWORD"))) {
      JdbcMetaData metadata = new JdbcMetaData(conn, List.of(conn.getSchema()));
      JSONObject node = new JSQLColumResolver(metadata)
          .getLineage(JSONObjectTreeBuilder.class,
              "select x.total from (select o.freight * 2 as total from orders o) x")
          .getJSONArray("columnSet").getJSONObject(0);
      assertEquals("x.total", node.getString("expression"));
      assertEquals("o.freight * 2", node.getString("definition"));
      JSONObject operation = node.getJSONArray("columnSet").getJSONArray(0).getJSONObject(0);
      assertEquals("operator", operation.getString("kind"));
      JSONObject freight = operation.getJSONArray("columnSet").getJSONArray(0).getJSONObject(0);
      assertEquals("column", freight.getString("kind"));
      assertEquals(conn.getCatalog() + "." + conn.getSchema() + ".orders",
          freight.getString("table"));
    }
  }

  @ParameterizedTest
  @CsvSource({"true,true", "true,false", "false,true", "false,false"})
  void scanPreservesTransaction(boolean autoCommit, boolean filtered) throws Exception {
    try (Connection conn = DriverManager.getConnection(System.getenv("POSTGRES_JDBC_URL"),
        System.getenv("POSTGRES_USER"), System.getenv("POSTGRES_PASSWORD"))) {
      String catalog = conn.getCatalog();
      String schema = conn.getSchema();
      conn.setAutoCommit(autoCommit);
      Savepoint beforeMarker = null;
      if (!autoCommit) {
        beforeMarker = conn.setSavepoint();
        try (Statement st = conn.createStatement()) {
          st.execute("CREATE TEMP TABLE transpiler_tx_marker (id INTEGER) ON COMMIT DROP");
          st.execute("INSERT INTO transpiler_tx_marker VALUES (7)");
        }
      }
      try {
        JdbcMetaData metadata =
            filtered ? new JdbcMetaData(conn, List.of(schema.toUpperCase(java.util.Locale.ROOT)))
                : new JdbcMetaData(conn);
        assertEquals(autoCommit, conn.getAutoCommit());
        assertEquals(catalog, metadata.getCurrentCatalogName());
        assertEquals(schema, metadata.getCurrentSchemaName());
        JdbcSchema extracted = metadata.get(catalog).get(schema);
        assertNotNull(extracted, "Driver's null schema catalog must map to the current database");
        assertFalse(extracted.tables.isEmpty(), "Test database must contain user tables");
        for (JdbcTable table : extracted.tables.values()) {
          assertFalse(table.columns.isEmpty(),
              "Missing columns: " + catalog + "." + schema + "." + table.tableName);
          for (JdbcColumn column : table.columns.values()) {
            assertEquals(catalog, column.tableCatalog, "Column catalog: " + column.columnName);
          }
        }
        System.out
            .println("PostgreSQL " + conn.getMetaData().getDatabaseProductVersion() + ", driver "
                + conn.getMetaData().getDriverVersion() + ", filtered=" + filtered + ", autoCommit="
                + autoCommit + ", user tables=" + extracted.tables.size() + ", user columns="
                + extracted.tables.values().stream().mapToInt(table -> table.columns.size()).sum());
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery("SELECT 1")) {
          if (!rs.next()) {
            fail("Expected a result row");
          }
          assertEquals(1, rs.getInt(1));
        }
        // Public metadata helpers must also avoid SQL probes on PostgreSQL.
        assertFalse(JdbcCatalog.getCatalogsFromInformationSchema(conn).isEmpty());
        assertFalse(JdbcSchema.getSchemasFromInformationSchema(conn).isEmpty());
        assertFalse(JdbcTable
            .getTablesFromInformationSchema(conn.getMetaData(), catalog, schema, "%").isEmpty());
        assertFalse(
            JdbcTable.getColumnsFromSchemaInformation(conn, catalog, schema, "%").isEmpty());
        if (!autoCommit) {
          try (Statement st = conn.createStatement();
              ResultSet rs = st.executeQuery("SELECT id FROM transpiler_tx_marker")) {
            if (!rs.next()) {
              fail("Scan must not roll back prior work");
            }
            assertEquals(7, rs.getInt(1));
          }
          conn.rollback(beforeMarker);
          try (Statement st = conn.createStatement();
              ResultSet rs =
                  st.executeQuery("SELECT to_regclass('pg_temp.transpiler_tx_marker')")) {
            if (!rs.next()) {
              fail("Expected a result row");
            }
            assertNull(rs.getString(1), "Original savepoint must remain usable after scanning");
          }
        }
      } finally {
        if (!autoCommit) {
          conn.rollback();
        }
      }
    }
  }
}
