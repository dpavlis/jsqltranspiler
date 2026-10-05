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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class JdbcSchemaFilterTest {
  private Connection conn;

  @BeforeEach
  void setUp() throws SQLException {
    conn = DriverManager.getConnection("jdbc:h2:mem:");
    try (Statement st = conn.createStatement()) {
      for (String schema : List.of("SALES", "STAGE", "STAGE2", "A_B", "A%B", "A\\B")) {
        st.execute("CREATE SCHEMA \"" + schema + "\"");
      }
      st.execute("CREATE TABLE SALES.ORDERS(ID INT, AMOUNT DECIMAL(10,2))");
      st.execute("CREATE TABLE SALES.CUSTOMERS(ID INT, NAME VARCHAR(30))");
      st.execute("CREATE TABLE STAGE.ORDERS_RAW(ID INT)");
      st.execute("CREATE TABLE STAGE2.X(ID INT)");
      for (String schema : List.of("A_B", "A%B", "A\\B")) {
        st.execute("CREATE TABLE \"" + schema + "\".T(ID INT)");
      }
    }
  }

  @AfterEach
  void tearDown() throws SQLException {
    conn.close();
  }

  private List<String> tables(JdbcMetaData metadata) {
    return metadata.getCatalogsList().stream().flatMap(c -> c.schemas.values().stream())
        .flatMap(s -> s.tables.values().stream()).map(t -> t.tableSchema + "." + t.tableName)
        .sorted().collect(Collectors.toList());
  }

  @Test
  void quotedCatalogNamesTreatWildcardCharactersLiterally() throws SQLException {
    assertArrayEquals(new String[] {"SNOWFLAKE_SAMPLE_DATA", "TPCH_SF1"},
        JdbcMetaData.parseSchemaPattern("\"SNOWFLAKE_SAMPLE_DATA\".TPCH_SF1"));
    assertArrayEquals(new String[] {"A%B", "PUBLIC"},
        JdbcMetaData.parseSchemaPattern("\"A%B\".PUBLIC"));
    assertArrayEquals(new String[] {"SNOWFLAKE_SAMPLE_DATA", "TPCH_SF1"},
        JdbcMetaData.parseSchemaPattern("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1"));
    assertArrayEquals(new String[] {"A%B", "PUBLIC"},
        JdbcMetaData.parseSchemaPattern("A%B.PUBLIC"));
    assertArrayEquals(new String[] {"my_db", "stage%"},
        JdbcMetaData.parseSchemaPattern("my_db.stage%"));
    assertArrayEquals(new String[] {"my.db", "public"},
        JdbcMetaData.parseSchemaPattern("\"my.db\".public"));
    String quotedCatalog = "\"" + conn.getCatalog().replace("\"", "\"\"") + "\"";
    assertEquals(List.of("SALES.CUSTOMERS", "SALES.ORDERS"),
        tables(new JdbcMetaData(conn, List.of(quotedCatalog + ".SALES"))));
    assertEquals(List.of("SALES.CUSTOMERS", "SALES.ORDERS"),
        tables(new JdbcMetaData(conn, List.of(conn.getCatalog() + ".SALES"))));
  }

  @Test
  void unquotedCatalogWithUnderscoreExtractsSchemas() throws SQLException {
    try (Connection named = DriverManager.getConnection("jdbc:h2:mem:TEST_DB");
        Statement st = named.createStatement()) {
      st.execute("CREATE SCHEMA STAGE");
      st.execute("CREATE TABLE STAGE.T(ID INT)");
      JdbcMetaData md = new JdbcMetaData(named, List.of("TEST_DB.STAGE"));
      assertNotNull(md.get("TEST_DB").get("STAGE").get("T"));
    }
  }

  @Test
  void filtersAndCompatibility() throws SQLException {
    JdbcMetaData all = new JdbcMetaData(conn);
    assertTrue(tables(all)
        .containsAll(List.of("SALES.ORDERS", "SALES.CUSTOMERS", "STAGE.ORDERS_RAW", "STAGE2.X")));
    assertFalse(tables(all).stream().anyMatch(t -> t.startsWith("INFORMATION_SCHEMA.")));
    assertEquals(tables(all), tables(new JdbcMetaData(conn, null)));
    assertEquals(tables(all), tables(new JdbcMetaData(conn, List.of())));
    assertEquals(tables(all), tables(new JdbcMetaData(conn, List.of("%"))));
    for (List<String> patterns : List.of(List.of("SALES"), List.of("sales"),
        List.of(conn.getCatalog().toLowerCase() + ".sales"), List.of("SALES", "SAL%"))) {
      JdbcMetaData selected = new JdbcMetaData(conn, patterns);
      assertEquals(List.of("SALES.CUSTOMERS", "SALES.ORDERS"), tables(selected));
      assertNull(selected.get(conn.getCatalog()).get("STAGE"));
      assertNull(selected.get(conn.getCatalog()).get("STAGE2"));
      assertEquals(2, selected.get(conn.getCatalog()).get("SALES").get("ORDERS").columns.size());
      assertEquals(all.getCurrentCatalogName(), selected.getCurrentCatalogName());
      assertEquals(all.getCurrentSchemaName(), selected.getCurrentSchemaName());
    }
    assertEquals(List.of("STAGE.ORDERS_RAW", "STAGE2.X"),
        tables(new JdbcMetaData(conn, List.of("STAGE%"))));
    assertEquals(List.of("STAGE2.X"), tables(new JdbcMetaData(conn, List.of("STAGE_"))));
    for (String pattern : List.of("NOPE", "INFORMATION_SCHEMA")) {
      JdbcMetaData selected = new JdbcMetaData(conn, List.of(pattern));
      assertTrue(tables(selected).isEmpty());
      assertEquals(List.of(""), new ArrayList<>(selected.keySet()));
      assertNotNull(selected.get("").get(""));
    }
  }

  @Test
  void parsesPatterns() {
    assertArrayEquals(new String[] {null, "public"}, JdbcMetaData.parseSchemaPattern("public"));
    assertArrayEquals(new String[] {"dwh", "public"},
        JdbcMetaData.parseSchemaPattern("dwh.public"));
    assertArrayEquals(new String[] {"my.db", "public"},
        JdbcMetaData.parseSchemaPattern("\"my.db\".public"));
    assertArrayEquals(new String[] {"my\"db", "pub.lic"},
        JdbcMetaData.parseSchemaPattern("\"my\"\"db\".\"pub.lic\""));
    assertArrayEquals(new String[] {"a.b", "c"}, JdbcMetaData.parseSchemaPattern("a.b.c"));
    for (String invalid : Arrays.asList(null, "", "  ", ".public", "dwh.", "\"dwh.public",
        "dw\"h.public", "\"a\"x.public", "\"\"")) {
      assertThrows(IllegalArgumentException.class, () -> JdbcMetaData.parseSchemaPattern(invalid));
    }
  }

  @Test
  void literalWildcardsAndFallback() throws SQLException {
    List<String> calls = new ArrayList<>();
    Connection fallback = wrapConnection(calls, true, null);
    for (Connection connection : List.of(conn, fallback)) {
      for (String pattern : List.of("A\\_B", "A\\%B", "A\\\\B")) {
        JdbcMetaData selected = new JdbcMetaData(connection, List.of(pattern));
        assertEquals(1, tables(selected).size());
        JdbcTable table = selected.get(conn.getCatalog()).schemas.values().stream()
            .flatMap(s -> s.tables.values().stream()).findFirst().orElseThrow();
        assertEquals(1, table.columns.size());
      }
      assertEquals(List.of("SALES.CUSTOMERS", "SALES.ORDERS"),
          tables(new JdbcMetaData(connection, List.of("sales"))));
    }
    assertTrue(calls.contains("getTables:" + conn.getCatalog() + ":SALES:%"));
    assertTrue(calls.contains("getColumns:" + conn.getCatalog() + ":SALES:%"));
    assertTrue(calls.stream().anyMatch(c -> c.contains("A\\_B")));
  }

  @Test
  void directHelpersBindFiltersAndQuoteCatalogs() throws SQLException {
    List<String> calls = new ArrayList<>();
    DatabaseMetaData md = wrapConnection(calls, false, null).getMetaData();
    assertEquals(1, JdbcTable.getTables(md, conn.getCatalog(), "STAGE_", "%").size());
    assertEquals(1, JdbcTable.getColumns(md, conn.getCatalog(), "STAGE_", "%").size());
    assertTrue(calls.stream().anyMatch(c -> c.contains("TABLE_SCHEMA LIKE ?")));
    assertTrue(calls.contains("bind:STAGE_"));
    assertTrue(calls.stream().anyMatch(c -> c.contains("TABLE_CATALOG = ?")));
    calls.clear();
    JdbcTable.getTables(md, conn.getCatalog(), "SALES", "x' OR 1=1 --");
    JdbcTable.getTables(md, "my\"cat", "SALES", "%");
    assertTrue(calls.stream().anyMatch(c -> c.contains("\"my\"\"cat\".information_schema.tables")));
    assertTrue(calls.contains("bind:x' OR 1=1 --"));
    assertFalse(
        calls.stream().filter(c -> c.startsWith("sql:")).anyMatch(c -> c.contains("OR 1=1")));
    calls.clear();
    JdbcTable.getTables(wrapConnection(calls, false, "").getMetaData(), null, null, null);
    assertTrue(calls
        .contains("sql:SELECT * FROM information_schema.tables WHERE 1=1 AND TABLE_NAME LIKE ?"));
    assertTrue(calls.contains("bind:%"));
  }

  @Test
  void jsonRoundTrip() throws SQLException {
    JdbcMetaData selected = new JdbcMetaData(conn, List.of("SALES"));
    org.json.JSONObject json = JdbcJSONSerializer.toJson(selected);
    for (boolean extraKey : List.of(false, true)) {
      if (extraKey) {
        json.put("format", "x");
      }
      JdbcMetaData restored = JdbcJSONSerializer.fromJson(new StringReader(json.toString()));
      assertEquals(selected.getCatalogsList(), restored.getCatalogsList());
      assertEquals(tables(selected), tables(restored));
    }
  }

  @Test
  void postgresRoutingAvoidsProbesAndPreservesCallerWork() throws SQLException {
    Connection[] wrapped = new Connection[1];
    DatabaseMetaData md = proxy(DatabaseMetaData.class, conn.getMetaData(), (method, args) -> {
      if (method.equals("getDatabaseProductName")) {
        return "PostgreSQL";
      }
      if (method.equals("getConnection")) {
        return wrapped[0];
      }
      if (method.equals("getColumns")) {
        ResultSet columns = conn.getMetaData().getColumns((String) args[0], (String) args[1],
            (String) args[2], (String) args[3]);
        return proxy(ResultSet.class, columns, (name, parameters) -> {
          if (name.equals("getString") && "TABLE_CAT".equals(parameters[0])) {
            return null;
          }
          return UNHANDLED;
        });
      }
      if (method.equals("getTables")) {
        ResultSet tables = conn.getMetaData().getTables((String) args[0], (String) args[1],
            (String) args[2], (String[]) args[3]);
        return proxy(ResultSet.class, tables, (name, parameters) -> {
          if (name.equals("getString") && "TABLE_CAT".equals(parameters[0])) {
            return null;
          }
          return UNHANDLED;
        });
      }
      if (method.equals("getSchemas")) {
        return proxy(ResultSet.class, conn.getMetaData().getSchemas(), (name, parameters) -> {
          if (name.equals("getString") && "TABLE_CATALOG".equals(parameters[0])) {
            return null;
          }
          return UNHANDLED;
        });
      }
      return UNHANDLED;
    });
    wrapped[0] = proxy(Connection.class, conn, (method, args) -> {
      if (method.equals("getMetaData")) {
        return md;
      }
      if (method.equals("setAutoCommit") || method.equals("commit") || method.equals("rollback")) {
        throw new AssertionError("Metadata extraction changed caller transaction: " + method);
      }
      if (method.equals("prepareStatement")) {
        throw new AssertionError("PostgreSQL must use JDBC metadata without SQL probes");
      }
      if (method.equals("createStatement")) {
        Statement statement = conn.createStatement();
        return proxy(Statement.class, statement, (name, parameters) -> {
          if (name.equals("executeQuery")) {
            assertEquals("SELECT current_database(), current_schema()", parameters[0]);
            return statement.executeQuery("SELECT current_catalog(), current_schema()");
          }
          return UNHANDLED;
        });
      }
      return UNHANDLED;
    });
    conn.setAutoCommit(false);
    Savepoint beforeInsert = conn.setSavepoint();
    try (Statement st = conn.createStatement()) {
      st.execute("INSERT INTO SALES.ORDERS VALUES (7, 10)");
    }
    for (JdbcMetaData metadata : List.of(new JdbcMetaData(wrapped[0]),
        new JdbcMetaData(wrapped[0], List.of("sales")))) {
      assertNotNull(metadata.get(conn.getCatalog()).get("SALES"));
      assertEquals(2, metadata.get(conn.getCatalog()).get("SALES").tables.size());
      assertEquals(2, metadata.get(conn.getCatalog()).get("SALES").get("ORDERS").columns.size());
    }
    assertFalse(conn.getAutoCommit());
    assertFalse(JdbcCatalog.getCatalogsFromInformationSchema(wrapped[0]).isEmpty());
    assertFalse(JdbcSchema.getSchemasFromInformationSchema(wrapped[0]).isEmpty());
    assertEquals(2,
        JdbcTable.getTablesFromInformationSchema(md, conn.getCatalog(), "SALES", "%").size());
    assertEquals(4, JdbcTable
        .getColumnsFromSchemaInformation(wrapped[0], conn.getCatalog(), "SALES", "%").size());
    try (Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM SALES.ORDERS")) {
      if (!rs.next()) {
        fail("Expected a result row");
      }
      assertEquals(1, rs.getInt(1));
    }
    conn.rollback(beforeInsert);
    try (Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM SALES.ORDERS")) {
      if (!rs.next()) {
        fail("Expected a result row");
      }
      assertEquals(0, rs.getInt(1));
    }
    conn.rollback();
  }

  private Connection wrapConnection(List<String> calls, boolean failInformationSchema,
      String catalogOverride) throws SQLException {
    Connection[] wrapped = new Connection[1];
    DatabaseMetaData delegate = conn.getMetaData();
    DatabaseMetaData md = proxy(DatabaseMetaData.class, delegate, (method, args) -> {
      if (method.equals("getConnection")) {
        return wrapped[0];
      }
      if (method.equals("getTables") || method.equals("getColumns")) {
        calls.add(method + ":" + args[0] + ":" + args[1] + ":" + args[2]);
      }
      return UNHANDLED;
    });
    wrapped[0] = proxy(Connection.class, conn, (method, args) -> {
      if (method.equals("getMetaData")) {
        return md;
      }
      if (method.equals("getCatalog") && catalogOverride != null) {
        return catalogOverride.isEmpty() ? null : catalogOverride;
      }
      if (method.equals("prepareStatement")) {
        String sql = (String) args[0];
        calls.add("sql:" + sql);
        if (failInformationSchema && sql.toLowerCase().contains("information_schema")) {
          throw new SQLException("Forced INFORMATION_SCHEMA failure");
        }
        return proxy(PreparedStatement.class, conn.prepareStatement(sql), (name, parameters) -> {
          if (name.equals("setObject")) {
            calls.add("bind:" + parameters[1]);
          }
          return UNHANDLED;
        });
      }
      if (method.equals("createStatement") && failInformationSchema) {
        return proxy(Statement.class, conn.createStatement(), (name, parameters) -> {
          if (name.equals("executeQuery")
              && ((String) parameters[0]).toLowerCase().contains("information_schema")) {
            throw new SQLException("Forced INFORMATION_SCHEMA failure");
          }
          return UNHANDLED;
        });
      }
      return UNHANDLED;
    });
    return wrapped[0];
  }

  private static final Object UNHANDLED = new Object();

  private interface Interceptor {
    Object invoke(String method, Object[] args) throws SQLException;
  }

  private static <T> T proxy(Class<T> type, T delegate, Interceptor interceptor) {
    return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type},
        (object, method, args) -> {
          Object result = interceptor.invoke(method.getName(), args);
          if (result != UNHANDLED) {
            return result;
          }
          try {
            return method.invoke(delegate, args);
          } catch (InvocationTargetException ex) {
            throw ex.getCause();
          }
        }));
  }
}
