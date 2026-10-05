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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;
import java.sql.ResultSet;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Live PostgreSQL regressions configured through the local, ignored properties file. */
class PostgreSqlMetaDataTest {
  @Test
  void northwindExpressionLineage() throws Exception {
    try (Connection conn = LiveDatabaseProperties.connect("postgresql")) {
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

  @Test
  void bulkCatalogKeysPreserveTransactionAndHaveConstantQueryCount() throws Exception {
    try (Connection conn = LiveDatabaseProperties.connect("postgresql")) {
      conn.setAutoCommit(false);
      Savepoint before = conn.setSavepoint();
      String schema = "transpiler_keys_" + java.util.UUID.randomUUID().toString().replace("-", "");
      try {
        try (Statement st = conn.createStatement()) {
          st.execute("CREATE SCHEMA " + schema);
          st.execute("CREATE TABLE " + schema + ".customers(customer_id INT PRIMARY KEY)");
          st.execute("CREATE TABLE " + schema + ".products(product_id INT PRIMARY KEY)");
          st.execute("CREATE TABLE " + schema + ".orders(order_id SERIAL PRIMARY KEY, "
              + "customer_id INT REFERENCES " + schema + ".customers)");
          st.execute("CREATE TABLE " + schema + ".order_details(order_id INT, product_id INT, "
              + "PRIMARY KEY(order_id,product_id), FOREIGN KEY(order_id) REFERENCES " + schema
              + ".orders ON DELETE CASCADE, FOREIGN KEY(product_id) REFERENCES " + schema
              + ".products)");
          st.execute("COMMENT ON COLUMN " + schema + ".orders.order_id IS 'Order key'");
          st.execute("INSERT INTO " + schema + ".customers VALUES (42)");
        }
        Savepoint caller = conn.setSavepoint();
        AtomicInteger queries = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();
        AtomicInteger fkCalls = new AtomicInteger();
        Connection measured = countMetadataQueries(conn, queries, failures, fkCalls);
        JdbcMetaData small =
            new JdbcMetaData(measured, List.of(schema), JdbcMetaDataOptions.defaults());
        int smallCount = queries.get();
        JdbcTable details = small.get(conn.getCatalog()).get(schema).get("order_details");
        assertEquals(List.of("order_id", "product_id"), details.primaryKey.getColumnNames());
        assertEquals(2, details.foreignKeys.size());
        assertTrue(details.foreignKeys.stream().anyMatch(key -> key.deleteRule != null
            && key.deleteRule == DatabaseMetaData.importedKeyCascade));
        JdbcColumn id =
            small.get(conn.getCatalog()).get(schema).get("orders").columns.get("order_id");
        assertEquals("YES", id.isAutomaticIncrement);
        assertEquals("Order key", id.remarks);
        assertEquals(1, fkCalls.get());
        try (Statement st = conn.createStatement()) {
          for (int i = 0; i < 20; i++) {
            st.execute("CREATE TABLE " + schema + ".many_" + i
                + "(id INT PRIMARY KEY, customer_id INT REFERENCES " + schema + ".customers)");
          }
        }
        queries.set(0);
        fkCalls.set(0);
        JdbcMetaData large =
            new JdbcMetaData(measured, List.of(schema), JdbcMetaDataOptions.defaults());
        assertEquals(24, large.get(conn.getCatalog()).get(schema).tables.size());
        assertEquals(smallCount, queries.get(),
            "Metadata round trips must be independent of table count");
        assertEquals(1, fkCalls.get());
        assertEquals(0, failures.get(), "No failing SQL probe is allowed on PostgreSQL");
        assertFalse(conn.getAutoCommit());
        conn.rollback(caller);
        try (Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("SELECT customer_id FROM " + schema + ".customers")) {
          if (!rs.next()) {
            fail("Scan must preserve caller work");
          }
          assertEquals(42, rs.getInt(1));
        }
        conn.rollback(before);
        System.out.println("PostgreSQL bulk catalog keys: " + smallCount
            + " metadata queries for both 4 and 24 tables; no failed statements");
      } finally {
        conn.rollback();
      }
    }
  }

  @Test
  void selectOnlyRoleStillReadsPrimaryKeysInBulk() throws Exception {
    try (Connection conn = LiveDatabaseProperties.connect("postgresql")) {
      conn.setAutoCommit(false);
      String suffix = java.util.UUID.randomUUID().toString().replace("-", "");
      String schema = "transpiler_ro_" + suffix;
      String role = "transpiler_ro_" + suffix;
      try (Statement st = conn.createStatement()) {
        try (ResultSet rs = st.executeQuery(
            "SELECT rolcreaterole OR rolsuper FROM pg_roles WHERE rolname = current_user")) {
          org.junit.jupiter.api.Assumptions.assumeTrue(rs.next() && rs.getBoolean(1),
              "Live PostgreSQL user cannot create roles");
        }
        // Everything below, including the role, disappears with the final rollback.
        st.execute("CREATE SCHEMA " + schema);
        st.execute("CREATE TABLE " + schema + ".parent(id INT PRIMARY KEY, code INT UNIQUE)");
        st.execute("CREATE TABLE " + schema + ".child(id INT, k INT, PRIMARY KEY(k, id))");
        st.execute("CREATE ROLE " + role + " NOLOGIN");
        st.execute("GRANT USAGE ON SCHEMA " + schema + " TO " + role);
        st.execute("GRANT SELECT ON ALL TABLES IN SCHEMA " + schema + " TO " + role);
        // Non-superusers with CREATEROLE must be a member to switch (PostgreSQL 16+ only
        // self-grants ADMIN, without SET).
        st.execute("GRANT " + role + " TO CURRENT_USER");
        st.execute("SET LOCAL ROLE " + role);
        // information_schema.table_constraints hides constraints from SELECT-only users.
        try (ResultSet rs = st.executeQuery(
            "SELECT count(*) FROM information_schema.table_constraints WHERE table_schema = '"
                + schema + "'")) {
          assertTrue(rs.next());
          assertEquals(0, rs.getInt(1), "Precondition: constraints hidden from the role");
        }
        AtomicInteger queries = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();
        AtomicInteger fkCalls = new AtomicInteger();
        JdbcMetaData metadata =
            new JdbcMetaData(countMetadataQueries(conn, queries, failures, fkCalls),
                List.of(schema), JdbcMetaDataOptions.defaults());
        JdbcSchema scanned = metadata.get(conn.getCatalog()).get(schema);
        assertEquals(List.of("id"), scanned.get("parent").primaryKey.getColumnNames());
        assertEquals(List.of("k", "id"), scanned.get("child").primaryKey.getColumnNames());
        assertEquals(0, failures.get(), "No failing SQL probe is allowed on PostgreSQL");
      } finally {
        conn.rollback();
      }
    }
  }

  private Object invokeJdbc(Object target, Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException ex) {
      throw ex.getCause();
    }
  }

  private Connection countMetadataQueries(Connection conn, AtomicInteger queries,
      AtomicInteger failures, AtomicInteger fkCalls) throws Exception {
    Connection[] measured = new Connection[1];
    DatabaseMetaData md = (DatabaseMetaData) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {DatabaseMetaData.class}, (proxy, method, args) -> {
          String name = method.getName();
          if ("getConnection".equals(name)) {
            return measured[0];
          }
          if ("getPrimaryKeys".equals(name)) {
            fail("Primary keys must be read in bulk");
          }
          if ("getImportedKeys".equals(name)) {
            assertNull(args[2], "Foreign keys must be read in bulk");
            fkCalls.incrementAndGet();
          }
          if (List.of("getCatalogs", "getSchemas", "getTables", "getColumns", "getImportedKeys")
              .contains(name)) {
            queries.incrementAndGet();
          }
          return invokeJdbc(conn.getMetaData(), method, args);
        });
    measured[0] = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {Connection.class}, (proxy, method, args) -> {
          if ("getMetaData".equals(method.getName())) {
            return md;
          }
          Object result = invokeJdbc(conn, method, args);
          if ("prepareStatement".equals(method.getName())
              || "createStatement".equals(method.getName())) {
            Class<?> api =
                "prepareStatement".equals(method.getName()) ? java.sql.PreparedStatement.class
                    : Statement.class;
            return Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {api},
                (p, m, a) -> {
                  if ("executeQuery".equals(m.getName())) {
                    queries.incrementAndGet();
                  }
                  try {
                    return invokeJdbc(result, m, a);
                  } catch (SQLException ex) {
                    failures.incrementAndGet();
                    throw ex;
                  }
                });
          }
          return result;
        });
    return measured[0];
  }

  @ParameterizedTest
  @CsvSource({"true,true", "true,false", "false,true", "false,false"})
  void scanPreservesTransaction(boolean autoCommit, boolean filtered) throws Exception {
    try (Connection conn = LiveDatabaseProperties.connect("postgresql")) {
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
