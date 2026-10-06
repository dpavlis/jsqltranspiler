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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SAP HANA keys from the SYS views, and the safeguard for drivers that return no keys for a null
 * table. H2 stands in for the database; a proxy reports the product name and driver behaviour.
 */
class SapHanaKeysTest {
  /** How the proxied driver answers getPrimaryKeys/getImportedKeys. */
  private enum Keys {
    /** Real per-table answers, nothing for a null table (HANA, Db2 LUW). */
    NONE_FOR_NULL_TABLE,
    /** Nothing for any table. */
    NONE,
    /** Pass every call through. */
    REAL
  }

  private Connection conn;
  private final List<String> keyCalls = new ArrayList<>();
  private final List<String> progress = new ArrayList<>();

  @BeforeEach
  void fixture() throws SQLException {
    conn = DriverManager.getConnection("jdbc:h2:mem:");
    try (Statement st = conn.createStatement()) {
      st.execute("CREATE SCHEMA LT");
      st.execute("CREATE TABLE LT.CUSTOMERS(ID INT PRIMARY KEY, NAME VARCHAR(50))");
      st.execute("CREATE TABLE LT.NOTES(TEXT VARCHAR(50))");
      st.execute("CREATE TABLE LT.ORDERS(ID INT PRIMARY KEY, CUSTOMER_ID INT, "
          + "CONSTRAINT LT_FK_ORD_CUST FOREIGN KEY(CUSTOMER_ID) REFERENCES LT.CUSTOMERS(ID))");
      st.execute("CREATE TABLE LT.ORDER_LINES(ORDER_ID INT, LINE_NO INT, QTY INT, "
          + "CONSTRAINT LT_PK_LINES PRIMARY KEY(ORDER_ID, LINE_NO), "
          + "CONSTRAINT LT_FK_LINES_ORD FOREIGN KEY(ORDER_ID) REFERENCES LT.ORDERS(ID) "
          + "ON DELETE CASCADE)");
    }
  }

  @AfterEach
  void close() throws SQLException {
    conn.close();
  }

  private void createSysViews() throws SQLException {
    try (Statement st = conn.createStatement()) {
      st.execute("CREATE SCHEMA SYS");
      st.execute("CREATE TABLE SYS.CONSTRAINTS(SCHEMA_NAME VARCHAR(128), TABLE_NAME VARCHAR(128), "
          + "COLUMN_NAME VARCHAR(128), POSITION SMALLINT, CONSTRAINT_NAME VARCHAR(128), "
          + "IS_PRIMARY_KEY VARCHAR(5), IS_UNIQUE_KEY VARCHAR(5))");
      // Rows deliberately out of POSITION order; a unique key and another schema must be ignored.
      st.execute("INSERT INTO SYS.CONSTRAINTS VALUES "
          + "('LT','ORDER_LINES','LINE_NO',2,'LT_PK_LINES','TRUE','TRUE'),"
          + "('LT','CUSTOMERS','ID',1,'_SYS_TREE_CS_#1_#0_#P0','TRUE','TRUE'),"
          + "('LT','ORDERS','ID',1,'_SYS_TREE_CS_#2_#0_#P0','TRUE','TRUE'),"
          + "('LT','ORDER_LINES','ORDER_ID',1,'LT_PK_LINES','TRUE','TRUE'),"
          + "('LT','CUSTOMERS','NAME',1,'LT_UQ_NAME','FALSE','TRUE'),"
          + "('OTHER','CUSTOMERS','CODE',1,'OTHER_PK','TRUE','TRUE')");
      st.execute("CREATE TABLE SYS.REFERENTIAL_CONSTRAINTS(SCHEMA_NAME VARCHAR(128), "
          + "TABLE_NAME VARCHAR(128), COLUMN_NAME VARCHAR(128), POSITION SMALLINT, "
          + "CONSTRAINT_NAME VARCHAR(128), REFERENCED_SCHEMA_NAME VARCHAR(128), "
          + "REFERENCED_TABLE_NAME VARCHAR(128), REFERENCED_COLUMN_NAME VARCHAR(128), "
          + "REFERENCED_CONSTRAINT_NAME VARCHAR(128), UPDATE_RULE VARCHAR(20), "
          + "DELETE_RULE VARCHAR(20), IS_ENFORCED VARCHAR(5), IS_VALIDATED VARCHAR(5), "
          + "CHECK_TIME VARCHAR(30))");
      st.execute("INSERT INTO SYS.REFERENTIAL_CONSTRAINTS VALUES "
          + "('LT','ORDERS','CUSTOMER_ID',1,'LT_FK_ORD_CUST','LT','CUSTOMERS','ID',"
          + "'_SYS_TREE_CS_#1_#0_#P0','RESTRICT','RESTRICT','TRUE','TRUE','INITIALLY_IMMEDIATE'),"
          + "('LT','ORDER_LINES','ORDER_ID',1,'LT_FK_LINES_ORD','LT','ORDERS','ID',"
          + "'_SYS_TREE_CS_#2_#0_#P0','SET NULL','CASCADE','TRUE','TRUE','INITIALLY_DEFERRED')");
    }
  }

  private JdbcMetaData scan() throws SQLException {
    return new JdbcMetaData(conn, List.of("LT"), JdbcMetaDataOptions.none());
  }

  private void enrich(JdbcMetaData metadata, String product, Keys keys) throws SQLException {
    JdbcKeyExtractor.enrich(profile(product, keys), metadata,
        JdbcMetaDataOptions.defaults().setComments(false).setColumnDetails(false)
            .withProgress((phase, catalog, schema, table, done, total) -> {
              assertTrue(done <= total);
              progress.add(phase + ":" + table + ":" + done + "/" + total);
            }));
  }

  private Connection profile(String product, Keys keys) throws SQLException {
    DatabaseMetaData delegate = conn.getMetaData();
    Connection[] profile = new Connection[1];
    DatabaseMetaData md = (DatabaseMetaData) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {DatabaseMetaData.class}, (proxy, method, args) -> {
          String name = method.getName();
          if ("getDatabaseProductName".equals(name)) {
            return product;
          }
          if ("getConnection".equals(name)) {
            return profile[0];
          }
          if ("getPrimaryKeys".equals(name) || "getImportedKeys".equals(name)) {
            keyCalls.add(name + ":" + args[2]);
            if (keys == Keys.NONE || keys == Keys.NONE_FOR_NULL_TABLE && args[2] == null) {
              Object[] none = {args[0], args[1], "NO_SUCH_TABLE"};
              return invoke(delegate, method, none);
            }
          }
          return invoke(delegate, method, args);
        });
    profile[0] = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {Connection.class}, (proxy, method,
            args) -> "getMetaData".equals(method.getName()) ? md : invoke(conn, method, args));
    return profile[0];
  }

  private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException ex) {
      throw ex.getCause();
    }
  }

  private JdbcTable table(JdbcMetaData metadata, String name) throws SQLException {
    return metadata.get(conn.getCatalog()).get("LT").get(name);
  }

  private List<String> calls(String method) {
    return keyCalls.stream().filter(call -> call.startsWith(method + ":"))
        .collect(Collectors.toList());
  }

  /** Tables in the order the extractor visits them. */
  private List<String> order(JdbcMetaData metadata) throws SQLException {
    return metadata.get(conn.getCatalog()).get("LT").tables.values().stream()
        .map(table -> table.tableName).collect(Collectors.toList());
  }

  private List<String> steps(String phase, List<String> tables, boolean accepted) {
    List<String> steps = new ArrayList<>();
    steps.add(phase + ":null:0/4");
    for (int i = 0; i < tables.size(); i++) {
      steps.add(phase + ":" + tables.get(i) + ":" + (i + 1) + "/4");
    }
    if (accepted) {
      steps.add(phase + ":null:4/4");
    }
    return steps;
  }

  private void assertAllKeys(JdbcMetaData metadata) throws SQLException {
    assertEquals(List.of("ID"), table(metadata, "CUSTOMERS").primaryKey.getColumnNames());
    assertEquals(List.of("ID"), table(metadata, "ORDERS").primaryKey.getColumnNames());
    assertEquals(List.of("ORDER_ID", "LINE_NO"),
        table(metadata, "ORDER_LINES").primaryKey.getColumnNames());
    assertNull(table(metadata, "NOTES").primaryKey);

    JdbcReference orders = table(metadata, "ORDERS").foreignKeys.get(0);
    assertEquals(1, table(metadata, "ORDERS").foreignKeys.size());
    assertEquals("LT", orders.getPkTableSchema());
    assertEquals("CUSTOMERS", orders.getPkTableName());
    assertArrayEquals(new String[] {"CUSTOMER_ID", "ID"}, orders.getColumns().get(0));
    JdbcReference lines = table(metadata, "ORDER_LINES").foreignKeys.get(0);
    assertEquals(1, table(metadata, "ORDER_LINES").foreignKeys.size());
    assertEquals("ORDERS", lines.getPkTableName());
    assertArrayEquals(new String[] {"ORDER_ID", "ID"}, lines.getColumns().get(0));
    assertEquals(DatabaseMetaData.importedKeyCascade, lines.getDeleteRule().intValue());
    assertTrue(table(metadata, "CUSTOMERS").foreignKeys.isEmpty());
    assertTrue(table(metadata, "NOTES").foreignKeys.isEmpty());
  }

  @Test
  void hanaReadsKeysFromSysViewsWithoutJdbcKeyCalls() throws SQLException {
    createSysViews();
    JdbcMetaData metadata = scan();
    enrich(metadata, "HDB", Keys.NONE_FOR_NULL_TABLE);
    assertEquals(List.of(), keyCalls, "Keys must come from the SYS views");
    assertAllKeys(metadata);
    assertEquals("_SYS_TREE_CS_#1_#0_#P0",
        table(metadata, "CUSTOMERS").primaryKey.getPrimaryKeyName());
    assertEquals("LT_PK_LINES", table(metadata, "ORDER_LINES").primaryKey.getPrimaryKeyName());

    JdbcReference orders = table(metadata, "ORDERS").foreignKeys.get(0);
    assertEquals("LT_FK_ORD_CUST", orders.getFkName());
    assertEquals(DatabaseMetaData.importedKeyRestrict, orders.getUpdateRule().intValue());
    assertEquals(DatabaseMetaData.importedKeyInitiallyImmediate,
        orders.getDeferrability().intValue());
    JdbcReference lines = table(metadata, "ORDER_LINES").foreignKeys.get(0);
    assertEquals(DatabaseMetaData.importedKeySetNull, lines.getUpdateRule().intValue());
    assertEquals(DatabaseMetaData.importedKeyInitiallyDeferred,
        lines.getDeferrability().intValue());
    assertEquals(List.of("PRIMARY_KEYS:null:0/4", "PRIMARY_KEYS:null:4/4", "FOREIGN_KEYS:null:0/4",
        "FOREIGN_KEYS:null:4/4"), progress);
  }

  @Test
  void hanaWithoutSysPrivilegeFallsBackToPerTableJdbc() throws SQLException {
    // No SYS views: the query fails as it would without the privilege on them.
    JdbcMetaData metadata = scan();
    enrich(metadata, "HDB", Keys.NONE_FOR_NULL_TABLE);
    assertAllKeys(metadata);
    // One schema-wide call, then every table once: none is read twice.
    List<String> expected = new ArrayList<>(List.of("null"));
    expected.addAll(order(metadata));
    assertEquals(expected, calls("getPrimaryKeys").stream()
        .map(call -> call.substring(call.indexOf(':') + 1)).collect(Collectors.toList()));
    assertEquals(expected, calls("getImportedKeys").stream()
        .map(call -> call.substring(call.indexOf(':') + 1)).collect(Collectors.toList()));
  }

  @Test
  void genericDriverIgnoringNullTableGetsKeysTableByTable() throws SQLException {
    JdbcMetaData metadata = scan();
    enrich(metadata, "AcmeDB", Keys.NONE_FOR_NULL_TABLE);
    assertAllKeys(metadata);
    List<String> tables = order(metadata);
    // A keyless table may come first: verification continues until a table has keys.
    assertNull(table(metadata, tables.get(0)).primaryKey, "Fixture must start keyless");
    List<String> expected = new ArrayList<>(steps("PRIMARY_KEYS", tables, false));
    expected.addAll(steps("FOREIGN_KEYS", tables, false));
    assertEquals(expected, progress);
  }

  @Test
  void emptySchemaWideAnswerCostsAtMostThreeTableChecks() throws SQLException {
    JdbcMetaData metadata = scan();
    enrich(metadata, "AcmeDB", Keys.NONE);
    assertEquals(4, calls("getPrimaryKeys").size(), "1 schema-wide + 3 table checks");
    assertEquals(4, calls("getImportedKeys").size(), "1 schema-wide + 3 table checks");
    for (JdbcTable table : metadata.get(conn.getCatalog()).get("LT").tables.values()) {
      assertNull(table.primaryKey);
      assertTrue(table.foreignKeys.isEmpty());
    }
    List<String> checked = order(metadata).subList(0, 3);
    List<String> expected = new ArrayList<>(steps("PRIMARY_KEYS", checked, true));
    expected.addAll(steps("FOREIGN_KEYS", checked, true));
    assertEquals(expected, progress);
  }

  @Test
  void authoritativeEmptyBulkAnswerIsNotVerified() throws SQLException {
    try (Statement st = conn.createStatement()) {
      st.execute("CREATE SCHEMA PLAIN");
      for (String name : List.of("A", "B", "C", "D")) {
        st.execute("CREATE TABLE PLAIN." + name + "(ID INT)");
      }
    }
    JdbcMetaData metadata = new JdbcMetaData(conn, List.of("PLAIN"), JdbcMetaDataOptions.none());
    // H2 itself: INFORMATION_SCHEMA strategy, whose empty answer is authoritative.
    enrich(metadata, conn.getMetaData().getDatabaseProductName(), Keys.REAL);
    assertEquals(List.of(), keyCalls, "No per-table key calls after a bulk answer");
  }
}
