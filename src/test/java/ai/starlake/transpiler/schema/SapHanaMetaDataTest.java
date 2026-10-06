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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Live SAP HANA (Cloud) key extraction, configured through the local, ignored properties file. */
class SapHanaMetaDataTest {
  private static final String SCHEMA = "JSQLT_KEYS";

  @BeforeAll
  static void createSchema() throws Exception {
    try (Connection conn = LiveDatabaseProperties.connect("hana");
        Statement st = conn.createStatement()) {
      dropSchema(st);
      st.execute("CREATE SCHEMA " + SCHEMA);
      st.execute("CREATE COLUMN TABLE " + SCHEMA
          + ".CUSTOMERS(ID INTEGER PRIMARY KEY, NAME NVARCHAR(50))");
      st.execute("CREATE COLUMN TABLE " + SCHEMA + ".ORDERS(ID INTEGER PRIMARY KEY, "
          + "CUSTOMER_ID INTEGER, CONSTRAINT JSQLT_FK_ORD_CUST FOREIGN KEY(CUSTOMER_ID) "
          + "REFERENCES " + SCHEMA + ".CUSTOMERS(ID))");
      st.execute("CREATE COLUMN TABLE " + SCHEMA + ".ORDER_LINES(LINE_NO INTEGER, "
          + "ORDER_ID INTEGER, QTY INTEGER, DISCOUNT DECIMAL(5, 2), "
          + "CONSTRAINT JSQLT_PK_LINES PRIMARY KEY(ORDER_ID, LINE_NO), "
          + "CONSTRAINT JSQLT_FK_LINES_ORD FOREIGN KEY(ORDER_ID) REFERENCES " + SCHEMA
          + ".ORDERS(ID) ON DELETE CASCADE)");
      st.execute("COMMENT ON TABLE " + SCHEMA + ".ORDER_LINES IS 'Order lines'");
      st.execute("COMMENT ON COLUMN " + SCHEMA + ".ORDER_LINES.DISCOUNT IS 'Line discount'");
    }
  }

  @AfterAll
  static void dropSchema() throws Exception {
    try (Connection conn = LiveDatabaseProperties.connect("hana");
        Statement st = conn.createStatement()) {
      dropSchema(st);
    }
  }

  private static void dropSchema(Statement st) {
    try {
      st.execute("DROP SCHEMA " + SCHEMA + " CASCADE");
    } catch (SQLException absent) {
      // Nothing left over from an earlier run.
    }
  }

  private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException ex) {
      throw ex.getCause();
    }
  }

  /** Records JDBC key calls and optionally rejects the SYS view queries. */
  private Connection observed(Connection conn, List<String> keyCalls, boolean denySysViews)
      throws SQLException {
    DatabaseMetaData delegate = conn.getMetaData();
    Connection[] observed = new Connection[1];
    DatabaseMetaData md = (DatabaseMetaData) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {DatabaseMetaData.class}, (proxy, method, args) -> {
          if ("getConnection".equals(method.getName())) {
            return observed[0];
          }
          if ("getPrimaryKeys".equals(method.getName())
              || "getImportedKeys".equals(method.getName())) {
            keyCalls.add(method.getName() + ":" + args[2]);
          }
          return invoke(delegate, method, args);
        });
    observed[0] = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {Connection.class}, (proxy, method, args) -> {
          if ("getMetaData".equals(method.getName())) {
            return md;
          }
          if (denySysViews && "prepareStatement".equals(method.getName())
              && ((String) args[0]).contains("SYS.")) {
            throw new SQLException("insufficient privilege: Not authorized", "HY000", 258);
          }
          return invoke(conn, method, args);
        });
    return observed[0];
  }

  private JdbcSchema extract(Connection conn) throws SQLException {
    JdbcMetaData metadata = new JdbcMetaData(conn, List.of(SCHEMA), JdbcMetaDataOptions.defaults());
    return metadata.getCatalogsList().stream().map(catalog -> catalog.get(SCHEMA))
        .filter(java.util.Objects::nonNull).findFirst()
        .orElseThrow(() -> new AssertionError("Schema " + SCHEMA + " not extracted"));
  }

  private void assertKeys(JdbcSchema schema) {
    assertEquals(3, schema.tables.size());
    assertEquals(List.of("ID"), schema.get("CUSTOMERS").primaryKey.getColumnNames());
    assertEquals(List.of("ID"), schema.get("ORDERS").primaryKey.getColumnNames());
    JdbcPrimaryKey lines = schema.get("ORDER_LINES").primaryKey;
    assertEquals(List.of("ORDER_ID", "LINE_NO"), lines.getColumnNames(), "Key order");
    assertEquals("JSQLT_PK_LINES", lines.getPrimaryKeyName());

    assertEquals(1, schema.get("ORDERS").foreignKeys.size());
    JdbcReference orders = schema.get("ORDERS").foreignKeys.get(0);
    assertEquals(SCHEMA, orders.getPkTableSchema());
    assertEquals("CUSTOMERS", orders.getPkTableName());
    assertEquals("JSQLT_FK_ORD_CUST", orders.getFkName());
    assertArrayEquals(new String[] {"CUSTOMER_ID", "ID"}, orders.getColumns().get(0));
    assertEquals(DatabaseMetaData.importedKeyRestrict, orders.getDeleteRule().intValue());

    assertEquals(1, schema.get("ORDER_LINES").foreignKeys.size());
    JdbcReference orderLines = schema.get("ORDER_LINES").foreignKeys.get(0);
    assertEquals("ORDERS", orderLines.getPkTableName());
    assertArrayEquals(new String[] {"ORDER_ID", "ID"}, orderLines.getColumns().get(0));
    assertEquals(DatabaseMetaData.importedKeyCascade, orderLines.getDeleteRule().intValue());
    assertTrue(schema.get("CUSTOMERS").foreignKeys.isEmpty());

    assertEquals("Order lines", schema.get("ORDER_LINES").remarks);
    assertEquals("Line discount", schema.get("ORDER_LINES").columns.get("DISCOUNT").remarks);
  }

  @ParameterizedTest(name = "autoCommit={0}")
  @ValueSource(booleans = {true, false})
  void keysComeFromSysViews(boolean autoCommit) throws Exception {
    try (Connection conn = LiveDatabaseProperties.connect("hana")) {
      conn.setAutoCommit(autoCommit);
      List<String> keyCalls = new ArrayList<>();
      JdbcSchema schema = extract(observed(conn, keyCalls, false));
      assertKeys(schema);
      assertEquals(List.of(), keyCalls, "Keys must be read in bulk from the SYS views");
      assertEquals(autoCommit, conn.getAutoCommit());
      if (!autoCommit) {
        conn.rollback();
      }
    }
  }

  @Test
  void withoutSysViewsDriverIgnoringNullTableIsReadPerTable() throws Exception {
    try (Connection conn = LiveDatabaseProperties.connect("hana")) {
      List<String> keyCalls = new ArrayList<>();
      JdbcSchema schema = extract(observed(conn, keyCalls, true));
      assertKeys(schema);
      // The HANA driver answers nothing for a null table; every table is then read once.
      assertEquals(2 * (1 + schema.tables.size()), keyCalls.size(), keyCalls.toString());
      assertTrue(keyCalls.contains("getPrimaryKeys:null"));
      assertTrue(keyCalls.contains("getImportedKeys:null"));
    }
  }
}
