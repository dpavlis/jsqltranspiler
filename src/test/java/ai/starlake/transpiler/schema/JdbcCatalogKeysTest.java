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

import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class JdbcCatalogKeysTest {
  private Connection conn;

  @BeforeEach
  void fixture() throws Exception {
    conn = DriverManager.getConnection("jdbc:h2:mem:");
    try (Statement st = conn.createStatement()) {
      st.execute("CREATE TABLE CUSTOMERS(CUSTOMER_ID INT PRIMARY KEY)");
      st.execute("CREATE TABLE PRODUCTS(PRODUCT_ID INT PRIMARY KEY)");
      st.execute("CREATE TABLE ORDERS(ORDER_ID INT PRIMARY KEY, CUSTOMER_ID INT, "
          + "CONSTRAINT FK_ORDERS_CUSTOMERS FOREIGN KEY(CUSTOMER_ID) REFERENCES CUSTOMERS)");
      st.execute("CREATE TABLE ORDER_DETAILS(ORDER_ID INT NOT NULL, PRODUCT_ID INT NOT NULL, "
          + "QUANTITY INT DEFAULT 1, CONSTRAINT PK_OD PRIMARY KEY(ORDER_ID, PRODUCT_ID), "
          + "CONSTRAINT FK_OD_ORDERS FOREIGN KEY(ORDER_ID) REFERENCES ORDERS ON DELETE CASCADE, "
          + "CONSTRAINT FK_OD_PRODUCTS FOREIGN KEY(PRODUCT_ID) REFERENCES PRODUCTS)");
      st.execute("CREATE INDEX IX_OD_PRODUCT ON ORDER_DETAILS(PRODUCT_ID)");
      st.execute("COMMENT ON TABLE ORDER_DETAILS IS 'Order lines'");
      st.execute("COMMENT ON COLUMN ORDER_DETAILS.ORDER_ID IS 'Order'");
      st.execute("CREATE TABLE NO_PK(ID INT GENERATED ALWAYS AS IDENTITY, "
          + "N INT, G INT GENERATED ALWAYS AS (N + 1), UNIQUE(N))");
      st.execute("CREATE SCHEMA STAGE");
      st.execute("CREATE TABLE STAGE.IMPORT(ID INT, CUSTOMER_ID INT, "
          + "CONSTRAINT FK_STAGE_CUSTOMERS FOREIGN KEY(CUSTOMER_ID) REFERENCES PUBLIC.CUSTOMERS)");
      st.execute("CREATE TABLE COMPOSITE_CHILD(A INT, B INT, "
          + "CONSTRAINT FK_REVERSED FOREIGN KEY(B,A) REFERENCES ORDER_DETAILS(ORDER_ID,PRODUCT_ID))");
    }
  }

  @AfterEach
  void close() throws Exception {
    conn.close();
  }

  private JdbcMetaData scan(JdbcMetaDataOptions options) throws Exception {
    return new JdbcMetaData(conn, List.of("PUBLIC"), options);
  }

  @Test
  void jdbcKeyFallbackUsesPerTableWhenNullTableIsRejected() throws Exception {
    JdbcMetaData metadata = scan(JdbcMetaDataOptions.none());
    AtomicInteger fkCalls = new AtomicInteger();
    DatabaseMetaData md = (DatabaseMetaData) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {DatabaseMetaData.class}, (proxy, method, args) -> {
          if ("getDatabaseProductName".equals(method.getName())) {
            return "DB2";
          }
          if ("getImportedKeys".equals(method.getName())) {
            fkCalls.incrementAndGet();
            if (args[2] == null) {
              throw new java.sql.SQLFeatureNotSupportedException("Null table unsupported");
            }
          }
          return invoke(conn.getMetaData(), method, args);
        });
    Connection profile = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {Connection.class}, (proxy, method, args) -> {
          if ("getMetaData".equals(method.getName())) {
            return md;
          }
          return invoke(conn, method, args);
        });
    JdbcKeyExtractor.enrich(profile, metadata,
        JdbcMetaDataOptions.defaults().setComments(false).setColumnDetails(false));
    assertEquals(metadata.get(conn.getCatalog()).get("PUBLIC").tables.size() + 1, fkCalls.get());
    assertEquals(1, table(metadata, "ORDERS").foreignKeys.size());
    assertEquals(2, table(metadata, "ORDER_DETAILS").foreignKeys.size());
    assertEquals(1, table(metadata, "COMPOSITE_CHILD").foreignKeys.size());
  }

  private JdbcTable table(JdbcMetaData md, String name) throws Exception {
    return md.get(conn.getCatalog()).get("PUBLIC").get(name);
  }

  private JSONObject jsonTable(JSONObject json, String schemaName, String tableName) {
    for (Object catalog : json.getJSONArray("catalogs")) {
      for (Object schema : ((JSONObject) catalog).getJSONArray("schemas")) {
        JSONObject s = (JSONObject) schema;
        if (schemaName.equals(s.getString("name"))) {
          for (Object table : s.getJSONArray("tables")) {
            if (tableName.equals(((JSONObject) table).getString("name"))) {
              return (JSONObject) table;
            }
          }
        }
      }
    }
    throw new AssertionError("Missing table " + schemaName + "." + tableName);
  }

  @Test
  void bulkKeysCommentsAndDetails() throws Exception {
    JdbcMetaData md = scan(JdbcMetaDataOptions.defaults());
    JdbcTable details = table(md, "ORDER_DETAILS");
    assertEquals(List.of("ORDER_ID", "PRODUCT_ID"), details.primaryKey.getColumnNames());
    assertEquals("PK_OD", details.primaryKey.getPrimaryKeyName());
    assertEquals(2, details.foreignKeys.size());
    JdbcReference orders = details.foreignKeys.stream()
        .filter(fk -> "ORDERS".equals(fk.getPkTableName())).findFirst().orElseThrow();
    assertEquals(DatabaseMetaData.importedKeyCascade, orders.getDeleteRule().intValue());
    assertEquals(conn.getCatalog(), orders.getPkTableCatalog());
    assertEquals("PUBLIC", orders.getPkTableSchema());
    assertArrayEquals(new String[] {"ORDER_ID", "ORDER_ID"}, orders.getColumns().get(0));
    assertNull(table(md, "NO_PK").primaryKey);
    assertEquals("Order lines", details.remarks);
    assertEquals("Order", details.columns.get("ORDER_ID").remarks);
    assertEquals(List.of(1, 2, 3),
        details.getColumns().stream().map(c -> c.ordinalPosition).toList());
    assertEquals("YES", table(md, "NO_PK").columns.get("ID").isAutomaticIncrement);
    assertEquals("YES", table(md, "NO_PK").columns.get("G").isGeneratedColumn);
    JdbcReference reversed = table(md, "COMPOSITE_CHILD").foreignKeys.get(0);
    assertArrayEquals(new String[] {"B", "ORDER_ID"}, reversed.getColumns().get(0));
    assertArrayEquals(new String[] {"A", "PRODUCT_ID"}, reversed.getColumns().get(1));
    JSONObject json = jsonTable(JdbcJSONSerializer.toJson(md), "PUBLIC", "ORDER_DETAILS");
    assertFalse(json.getJSONArray("columns").getJSONObject(0).getBoolean("isNullable"));
    assertTrue(json.getJSONArray("columns").getJSONObject(2).getBoolean("isNullable"));
    assertEquals("1", json.getJSONArray("columns").getJSONObject(2).getString("default"));
    assertFalse(json.has("indices"));
  }

  @Test
  void outsideSchemaReferencesAreRetained() throws Exception {
    JdbcMetaData md = new JdbcMetaData(conn, List.of("stage"), JdbcMetaDataOptions.defaults());
    assertNull(md.get(conn.getCatalog()).get("PUBLIC"));
    JdbcReference fk = md.get(conn.getCatalog()).get("STAGE").get("IMPORT").foreignKeys.get(0);
    assertEquals("PUBLIC", fk.getPkTableSchema());
    assertEquals("CUSTOMERS", fk.getPkTableName());
    assertEquals(conn.getCatalog(), fk.getPkTableCatalog());
  }

  @Test
  void roundTripIncludesAllNewFieldsAndIndicesAndCopies() throws Exception {
    JdbcMetaData original = scan(JdbcMetaDataOptions.defaults().setIndices(true));
    JSONObject json = JdbcJSONSerializer.toJson(original).put("format", "x");
    JdbcMetaData restored = JdbcJSONSerializer.fromJson(new StringReader(json.toString()));
    assertTrue(json.similar(JdbcJSONSerializer.toJson(restored).put("format", "x")));
    for (JdbcTable table : original.get(conn.getCatalog()).get("PUBLIC").tables.values()) {
      JdbcTable copy = table(restored, table.tableName);
      assertEquals(table.primaryKey, copy.primaryKey);
      assertEquals(table.foreignKeys, copy.foreignKeys);
      assertEquals(table.indices, copy.indices);
      assertEquals(table.remarks, copy.remarks);
      for (JdbcColumn column : table.columns.values()) {
        JdbcColumn c = copy.columns.get(column.columnName);
        assertEquals(column.remarks, c.remarks);
        assertEquals(column.columnDefinition, c.columnDefinition);
        assertEquals(column.ordinalPosition, c.ordinalPosition);
        assertEquals(column.isAutomaticIncrement, c.isAutomaticIncrement);
        assertEquals(column.isGeneratedColumn, c.isGeneratedColumn);
        assertEquals(column.isNullable, c.isNullable);
      }
    }
    JdbcIndex index = table(original, "ORDER_DETAILS").indices.get("IX_OD_PRODUCT");
    assertTrue(index.getNonUnique());
    assertEquals("PRODUCT_ID", index.getColumns().values().iterator().next().getColumnName());
    JdbcMetaData copied = JdbcMetaData.copyOf(original);
    assertEquals(table(original, "ORDER_DETAILS").foreignKeys,
        table(copied, "ORDER_DETAILS").foreignKeys);
    assertEquals(table(original, "ORDER_DETAILS").indices, table(copied, "ORDER_DETAILS").indices);
    table(copied, "ORDER_DETAILS").primaryKey.columnNames.clear();
    assertEquals(2, table(original, "ORDER_DETAILS").primaryKey.columnNames.size());
  }

  @Test
  void legacyConstructorsAndNoneKeepCompactJsonAndDoNotQueryKeys() throws Exception {
    AtomicInteger keyCalls = new AtomicInteger();
    Connection observed = observe(keyCalls, null, false, false);
    JSONObject old = JdbcJSONSerializer.toJson(new JdbcMetaData(observed));
    assertTrue(old.similar(JdbcJSONSerializer.toJson(new JdbcMetaData(observed, List.of()))));
    assertTrue(old.similar(JdbcJSONSerializer
        .toJson(new JdbcMetaData(observed, List.of(), JdbcMetaDataOptions.none()))));
    assertEquals(0, keyCalls.get());
    JSONObject table = jsonTable(old, "PUBLIC", "ORDER_DETAILS");
    assertEquals(java.util.Set.of("name", "type", "columns"), table.keySet());
    for (Object c : table.getJSONArray("columns")) {
      assertEquals(
          java.util.Set.of("name", "type", "typeID", "size", "decimalDigits", "isNullable"),
          ((JSONObject) c).keySet());
    }
    JdbcMetaData restored = JdbcJSONSerializer.fromJson(new StringReader(old.toString()));
    assertNull(table(restored, "ORDER_DETAILS").primaryKey);
    assertTrue(table(restored, "ORDER_DETAILS").foreignKeys.isEmpty());
    assertTrue(table(restored, "ORDER_DETAILS").indices.isEmpty());
  }

  @Test
  void unknownNullabilityIsOmittedAndRestoredAsUnknown() throws Exception {
    Connection missing = observe(new AtomicInteger(), null, false, true);
    JdbcMetaData md = new JdbcMetaData(missing, List.of("PUBLIC"));
    JSONObject json = JdbcJSONSerializer.toJson(md);
    JSONObject column =
        jsonTable(json, "PUBLIC", "ORDER_DETAILS").getJSONArray("columns").getJSONObject(0);
    assertFalse(column.has("isNullable"));
    JdbcMetaData restored = JdbcJSONSerializer.fromJson(new StringReader(json.toString()));
    assertEquals("", table(restored, "ORDER_DETAILS").columns.get("ORDER_ID").isNullable);
    assertEquals(DatabaseMetaData.columnNullableUnknown,
        table(restored, "ORDER_DETAILS").columns.get("ORDER_ID").nullable);
  }

  @Test
  void failedBulkProbesFallBackWithoutDisturbingCallerTransaction() throws Exception {
    conn.setAutoCommit(false);
    Savepoint caller = conn.setSavepoint();
    try (Statement st = conn.createStatement()) {
      st.execute("INSERT INTO CUSTOMERS VALUES(42)");
    }
    AtomicInteger calls = new AtomicInteger();
    Connection fallback = observe(calls, null, true, false);
    JdbcMetaData md = new JdbcMetaData(fallback, List.of("PUBLIC"), JdbcMetaDataOptions.defaults());
    assertEquals(7, calls.get());
    assertEquals(2, table(md, "ORDER_DETAILS").primaryKey.columnNames.size());
    assertEquals(2, table(md, "ORDER_DETAILS").foreignKeys.size());
    assertNull(table(md, "NO_PK").primaryKey);
    assertFalse(conn.getAutoCommit());
    try (Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM CUSTOMERS")) {
      if (!rs.next()) {
        fail("Expected count");
      }
      assertEquals(1, rs.getInt(1));
    }
    conn.rollback(caller);
  }

  @Test
  void bulkQueriesDoNotGrowWithTableCount() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    Connection measured = observe(new AtomicInteger(), calls, false, false);
    new JdbcMetaData(measured, List.of("PUBLIC"), JdbcMetaDataOptions.defaults());
    int small = calls.get();
    try (Statement st = conn.createStatement()) {
      for (int i = 0; i < 20; i++) {
        st.execute("CREATE TABLE MANY_" + i
            + "(ID INT PRIMARY KEY, CUSTOMER_ID INT REFERENCES CUSTOMERS)");
      }
    }
    calls.set(0);
    new JdbcMetaData(measured, List.of("PUBLIC"), JdbcMetaDataOptions.defaults());
    assertEquals(small, calls.get());
  }

  @Test
  void individualOptionsAreIndependentAndAreCopied() throws Exception {
    JdbcMetaDataOptions options = JdbcMetaDataOptions.none().withPrimaryKeys(true);
    JdbcMetaData md = scan(options);
    options.setPrimaryKeys(false).setIndices(true);
    JSONObject json = jsonTable(JdbcJSONSerializer.toJson(md), "PUBLIC", "ORDER_DETAILS");
    assertTrue(json.has("primaryKey"));
    assertFalse(json.has("foreignKeys"));
    assertFalse(json.has("indices"));
    assertFalse(json.has("remarks"));
    assertFalse(json.getJSONArray("columns").getJSONObject(0).has("ordinalPosition"));
    assertFalse(json.getJSONArray("columns").getJSONObject(0).has("remarks"));
    JdbcMetaData defaults = new JdbcMetaData(conn, List.of("PUBLIC"), null);
    assertEquals(2, table(defaults, "ORDER_DETAILS").foreignKeys.size());
    AtomicInteger keys = new AtomicInteger();
    JdbcMetaData unmatched = new JdbcMetaData(observe(keys, null, false, false), List.of("NOPE"),
        JdbcMetaDataOptions.defaults());
    assertEquals(0, keys.get());
    assertTrue(unmatched.getCatalogsList().stream().flatMap(c -> c.schemas.values().stream())
        .allMatch(schema -> schema.tables.isEmpty()));
  }

  @Test
  void driverWithoutSavepointsUsesNativeKeysAndPreservesWork() throws Exception {
    conn.setAutoCommit(false);
    Savepoint caller = conn.setSavepoint();
    Connection[] scoped = new Connection[1];
    DatabaseMetaData noSavepoints =
        (DatabaseMetaData) Proxy.newProxyInstance(getClass().getClassLoader(),
            new Class<?>[] {DatabaseMetaData.class}, (proxy, method, args) -> {
              if ("supportsSavepoints".equals(method.getName())) {
                return false;
              }
              if ("getConnection".equals(method.getName())) {
                return scoped[0];
              }
              return invoke(conn.getMetaData(), method, args);
            });
    scoped[0] = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {Connection.class}, (proxy, method, args) -> {
          if ("getMetaData".equals(method.getName())) {
            return noSavepoints;
          }
          if ("prepareStatement".equals(method.getName())
              || "createStatement".equals(method.getName())) {
            fail("No speculative SQL without savepoints");
          }
          return invoke(conn, method, args);
        });
    JdbcMetaData md =
        new JdbcMetaData(scoped[0], List.of("PUBLIC"), JdbcMetaDataOptions.defaults());
    assertEquals(2, table(md, "ORDER_DETAILS").primaryKey.columnNames.size());
    assertEquals(2, table(md, "ORDER_DETAILS").foreignKeys.size());
    assertFalse(conn.getAutoCommit());
    conn.rollback(caller);
  }

  @Test
  void columnsSerializeInOrdinalOrderAndAllRuleNamesRoundTrip() throws Exception {
    JdbcMetaData md = scan(JdbcMetaDataOptions.defaults());
    JdbcTable table = table(md, "ORDER_DETAILS");
    List<JdbcColumn> columns = table.getColumns();
    table.columns.clear();
    for (int i = columns.size() - 1; i >= 0; i--) {
      table.columns.put(columns.get(i).columnName, columns.get(i));
    }
    for (short rule = 0; rule <= 4; rule++) {
      table.foreignKeys.get(0).deleteRule = rule;
      JSONObject json = JdbcJSONSerializer.toJson(md);
      JSONObject t = jsonTable(json, "PUBLIC", "ORDER_DETAILS");
      assertEquals(List.of("ORDER_ID", "PRODUCT_ID", "QUANTITY"), t.getJSONArray("columns").toList()
          .stream().map(c -> ((java.util.Map<?, ?>) c).get("name")).toList());
      JdbcMetaData restored = JdbcJSONSerializer.fromJson(new StringReader(json.toString()));
      assertEquals(rule,
          table(restored, "ORDER_DETAILS").foreignKeys.get(0).deleteRule.shortValue());
    }
  }

  @Test
  void duckDbKeysAndCommentsStayInSelectedCatalogWithCollidingTableNames() throws Exception {
    try (Connection duck = DriverManager.getConnection("jdbc:duckdb:")) {
      try (Statement st = duck.createStatement()) {
        st.execute("CREATE TABLE main.t(id INT PRIMARY KEY)");
        st.execute("CREATE TABLE main.child(id INT REFERENCES main.t(id))");
        st.execute("COMMENT ON TABLE main.t IS 'Selected catalog'");
        st.execute("COMMENT ON COLUMN main.t.id IS 'Selected key'");
        st.execute("ATTACH ':memory:' AS other");
        st.execute("CREATE TABLE other.main.t(other_id VARCHAR PRIMARY KEY)");
        st.execute("COMMENT ON TABLE other.main.t IS 'Other catalog'");
      }
      String catalog = duck.getCatalog();
      JdbcMetaData md =
          new JdbcMetaData(duck, List.of(catalog + ".main"), JdbcMetaDataOptions.defaults());
      JdbcTable t = md.get(catalog).get("main").get("t");
      assertEquals(List.of("id"), t.primaryKey.getColumnNames());
      assertEquals("Selected catalog", t.remarks);
      assertEquals("Selected key", t.columns.get("id").remarks);
      assertNull(md.get("other"));
      JdbcReference fk = md.get(catalog).get("main").get("child").foreignKeys.get(0);
      assertEquals(catalog, fk.getPkTableCatalog());
      assertEquals("t", fk.getPkTableName());
      assertArrayEquals(new String[] {"id", "id"}, fk.getColumns().get(0));
    }
  }

  @Test
  void nativeColumnDetailsSupplementVendorInformationSchema() throws Exception {
    JdbcMetaData md = scan(JdbcMetaDataOptions.none());
    JdbcColumn generated = table(md, "NO_PK").columns.get("G");
    generated.isGeneratedColumn = "NO"; // Simulate an incomplete generic INFORMATION_SCHEMA result.
    Connection[] wrapped = new Connection[1];
    DatabaseMetaData vendor = (DatabaseMetaData) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {DatabaseMetaData.class}, (proxy, method, args) -> {
          if ("getConnection".equals(method.getName())) {
            return wrapped[0];
          }
          if ("getDatabaseProductName".equals(method.getName())) {
            return "Microsoft SQL Server";
          }
          return invoke(conn.getMetaData(), method, args);
        });
    wrapped[0] = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {Connection.class}, (proxy, method, args) -> {
          if ("getMetaData".equals(method.getName())) {
            return vendor;
          }
          return invoke(conn, method, args);
        });
    JdbcKeyExtractor.enrich(wrapped[0], md, JdbcMetaDataOptions.none().setColumnDetails(true));
    assertEquals("YES", generated.isGeneratedColumn);
    assertEquals("YES", table(md, "NO_PK").columns.get("ID").isAutomaticIncrement);
  }

  @Test
  void unnamedForeignKeysToSameTargetRemainDistinct() throws Exception {
    try (Statement st = conn.createStatement()) {
      st.execute("CREATE TABLE TWO_FKS(A INT, B INT, "
          + "CONSTRAINT FK_A FOREIGN KEY(A) REFERENCES CUSTOMERS, "
          + "CONSTRAINT FK_B FOREIGN KEY(B) REFERENCES CUSTOMERS)");
    }
    Connection base = observe(new AtomicInteger(), null, true, false);
    Connection[] wrapped = new Connection[1];
    DatabaseMetaData anonymous =
        (DatabaseMetaData) Proxy.newProxyInstance(getClass().getClassLoader(),
            new Class<?>[] {DatabaseMetaData.class}, (proxy, method, args) -> {
              if ("getConnection".equals(method.getName())) {
                return wrapped[0];
              }
              Object result = invoke(base.getMetaData(), method, args);
              if ("getImportedKeys".equals(method.getName())) {
                ResultSet rs = (ResultSet) result;
                return Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[] {ResultSet.class},
                    (p, m, a) -> "getString".equals(m.getName()) && "FK_NAME".equals(a[0]) ? null
                        : invoke(rs, m, a));
              }
              return result;
            });
    wrapped[0] = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {Connection.class}, (proxy, method, args) -> {
          if ("getMetaData".equals(method.getName())) {
            return anonymous;
          }
          return invoke(base, method, args);
        });
    JdbcMetaData md =
        new JdbcMetaData(wrapped[0], List.of("PUBLIC"), JdbcMetaDataOptions.defaults());
    List<JdbcReference> keys = table(md, "TWO_FKS").foreignKeys;
    assertEquals(2, keys.size());
    assertEquals(java.util.Set.of("A", "B"), keys.stream().map(key -> key.getColumns().get(0)[0])
        .collect(java.util.stream.Collectors.toSet()));
    assertTrue(keys.stream().allMatch(key -> key.getFkName() == null));
  }

  private Object invoke(Object target, Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException ex) {
      throw ex.getCause();
    }
  }

  private Connection observe(AtomicInteger keyCalls, AtomicInteger queryCalls, boolean failKeys,
      boolean unknownNullable) throws Exception {
    Connection[] wrapped = new Connection[1];
    DatabaseMetaData md = (DatabaseMetaData) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {DatabaseMetaData.class}, (proxy, method, args) -> {
          if ("getConnection".equals(method.getName())) {
            return wrapped[0];
          }
          if ("getPrimaryKeys".equals(method.getName())) {
            keyCalls.incrementAndGet();
          }
          Object result = invoke(conn.getMetaData(), method, args);
          if (unknownNullable && "getColumns".equals(method.getName())) {
            ResultSet rs = (ResultSet) result;
            return Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {ResultSet.class},
                (p, m, a) -> "getString".equals(m.getName()) && "IS_NULLABLE".equals(a[0]) ? ""
                    : invoke(rs, m, a));
          }
          return result;
        });
    wrapped[0] = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {Connection.class}, (proxy, method, args) -> {
          if ("getMetaData".equals(method.getName())) {
            return md;
          }
          if ("prepareStatement".equals(method.getName())) {
            String sql = (String) args[0];
            if (queryCalls != null) {
              queryCalls.incrementAndGet();
            }
            if (failKeys
                && (sql.contains("table_constraints") || sql.contains("referential_constraints"))) {
              throw new SQLException("Injected unsupported bulk key query");
            }
            if (unknownNullable && sql.contains("information_schema.columns")) {
              throw new SQLException("Use native columns to mock unknown nullability");
            }
          }
          return invoke(conn, method, args);
        });
    return wrapped[0];
  }
}
