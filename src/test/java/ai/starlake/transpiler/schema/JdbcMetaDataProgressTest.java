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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.stream.Collectors;

import static ai.starlake.transpiler.schema.JdbcMetaDataProgress.Phase.*;
import static org.junit.jupiter.api.Assertions.*;

class JdbcMetaDataProgressTest {
  private Connection conn;
  private final List<Event> events = new ArrayList<>();

  private record Event(JdbcMetaDataProgress.Phase phase, String catalog, String schema,
      String table, int done, int total) {}

  @BeforeEach
  void setUp() throws SQLException {
    conn = DriverManager.getConnection("jdbc:h2:mem:");
    try (Statement st = conn.createStatement()) {
      st.execute("CREATE SCHEMA SALES");
      st.execute("CREATE SCHEMA STAGE");
      st.execute("CREATE SCHEMA EMPTY_SCHEMA");
      st.execute("CREATE TABLE SALES.CUSTOMERS(ID INT PRIMARY KEY, NAME VARCHAR(30))");
      st.execute("CREATE TABLE SALES.ORDERS(ID INT PRIMARY KEY, CUSTOMER_ID INT "
          + "REFERENCES SALES.CUSTOMERS(ID))");
      st.execute("CREATE TABLE STAGE.RAW(ID INT)");
    }
  }

  @AfterEach
  void tearDown() throws SQLException {
    conn.close();
  }

  private void record(JdbcMetaDataProgress.Phase phase, String catalog, String schema, String table,
      int done, int total) {
    assertTrue(total < 0 || done <= total);
    events.add(new Event(phase, catalog, schema, table, done, total));
  }

  private List<Event> phase(JdbcMetaDataProgress.Phase phase) {
    return events.stream().filter(e -> e.phase == phase).collect(Collectors.toList());
  }

  private List<JdbcMetaDataProgress.Phase> starts() {
    return events.stream().filter(e -> e.done == 0).map(Event::phase).collect(Collectors.toList());
  }

  @Test
  void reportsOrderedPhasesAndCountsWithClosedResources() throws SQLException {
    Tracking tracking = new Tracking();
    conn.setAutoCommit(false);
    new JdbcMetaData(tracking.connection(), List.of("SALES", "STAGE"),
        JdbcMetaDataOptions.defaults().withProgress((p, c, s, t, d, n) -> {
          tracking.assertIdle();
          record(p, c, s, t, d, n);
        }));
    // Defaults enable comments and column details: both phases are reported per schema.
    assertEquals(List.of(CATALOGS, SCHEMAS, TABLES, COLUMNS, PRIMARY_KEYS, FOREIGN_KEYS, COMMENTS,
        COLUMN_DETAILS, PRIMARY_KEYS, FOREIGN_KEYS, COMMENTS, COLUMN_DETAILS), starts());
    assertEquals(4, phase(COLUMN_DETAILS).size());
    assertEquals(List.of(0, 1, 2),
        phase(TABLES).stream().map(Event::done).collect(Collectors.toList()));
    assertTrue(phase(TABLES).stream().allMatch(e -> e.total == 2));
    assertEquals(List.of("SALES", "STAGE"),
        phase(TABLES).stream().skip(1).map(Event::schema).sorted().collect(Collectors.toList()));
    assertTrue(phase(TABLES).stream().skip(1).allMatch(e -> e.catalog.equals(connCatalog())));
    assertEquals(new Event(DONE, null, null, null, 3, 3), phase(DONE).get(0));
    assertEquals(1, phase(DONE).size());
    assertTrue(phase(PRIMARY_KEYS).stream().allMatch(e -> e.table == null));
    List<Event> keys = phase(PRIMARY_KEYS);
    for (int i = 0; i < keys.size(); i += 2) {
      int tables = keys.get(i).schema.equals("SALES") ? 2 : 1;
      assertEquals(0, keys.get(i).done);
      assertEquals(tables, keys.get(i + 1).done);
      assertEquals(tables, keys.get(i + 1).total);
    }
    conn.rollback();
  }

  private String connCatalog() {
    try {
      return conn.getCatalog();
    } catch (SQLException ex) {
      throw new AssertionError(ex);
    }
  }

  @Test
  void noneAndEmptySchemasSkipEnrichment() throws SQLException {
    new JdbcMetaData(conn, List.of("SALES", "STAGE"),
        JdbcMetaDataOptions.none().setProgress(this::record));
    assertEquals(List.of(CATALOGS, SCHEMAS, TABLES, COLUMNS), starts());
    assertEquals(DONE, events.get(events.size() - 1).phase);
    events.clear();
    new JdbcMetaData(conn, List.of("EMPTY_SCHEMA"),
        JdbcMetaDataOptions.defaults().setProgress(this::record));
    assertEquals(List.of(CATALOGS, SCHEMAS, TABLES, COLUMNS, DONE), starts());
    assertEquals(0, phase(DONE).get(0).total);
  }

  @Test
  void detailsOnlyAndIndicesReportTheirUnits() throws SQLException {
    new JdbcMetaData(conn, List.of("SALES"), JdbcMetaDataOptions.none().setColumnDetails(true)
        .setIndices(true).setProgress(this::record));
    assertEquals(List.of(CATALOGS, SCHEMAS, TABLES, COLUMNS, COLUMN_DETAILS, INDICES), starts());
    assertEquals(List.of(0, 2),
        phase(COLUMN_DETAILS).stream().map(Event::done).collect(Collectors.toList()));
    assertEquals(List.of(0, 1, 2),
        phase(INDICES).stream().map(Event::done).collect(Collectors.toList()));
    assertTrue(phase(INDICES).stream().skip(1).allMatch(e -> e.table != null && e.total == 2));
  }

  @Test
  void unfilteredDiscoveryUsesOneUnit() throws SQLException {
    new JdbcMetaData(conn, null, JdbcMetaDataOptions.none().setProgress(this::record));
    for (JdbcMetaDataProgress.Phase p : List.of(TABLES, COLUMNS)) {
      assertEquals(
          List.of(new Event(p, null, null, null, 0, 1), new Event(p, null, null, null, 1, 1)),
          phase(p));
    }
    assertEquals(3, phase(DONE).get(0).total);
  }

  @Test
  void cancellationPropagatesUnchangedAndConnectionRemainsUsable() throws SQLException {
    for (boolean transactional : List.of(false, true)) {
      conn.setAutoCommit(!transactional);
      for (JdbcMetaDataProgress.Phase target : List.of(COLUMNS, PRIMARY_KEYS)) {
        for (boolean afterCall : List.of(false, true)) {
          events.clear();
          Tracking tracking = new Tracking();
          CancellationException stop = new CancellationException("stop");
          assertSame(stop,
              assertThrows(CancellationException.class,
                  () -> new JdbcMetaData(tracking.connection(), List.of("SALES", "STAGE"),
                      JdbcMetaDataOptions.defaults().setProgress((p, c, s, t, d, n) -> {
                        tracking.assertIdle();
                        record(p, c, s, t, d, n);
                        if (p == target && (afterCall ? d > 0 : d == 0)) {
                          throw stop;
                        }
                      }))));
          assertTrue(phase(DONE).isEmpty());
          assertEquals(target, events.get(events.size() - 1).phase);
          tracking.assertIdle();
          try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery("SELECT 1")) {
            if (rs.next()) {
              assertEquals(1, rs.getInt(1));
            } else {
              fail("Connection did not return SELECT 1 after cancellation");
            }
          }
          if (transactional) {
            conn.rollback();
          }
          assertNotNull(new JdbcMetaData(conn, List.of("SALES"), JdbcMetaDataOptions.defaults()));
        }
      }
    }
  }

  @Test
  void rejectedSchemaWideKeysReportPerTableFallback() throws SQLException {
    Tracking tracking = new Tracking();
    tracking.rejectBulkKeys = true;
    tracking.rejectSchemaKeys = true;
    conn.setAutoCommit(false);
    new JdbcMetaData(tracking.connection(), List.of("SALES"),
        JdbcMetaDataOptions.defaults().setProgress((p, c, s, t, d, n) -> {
          tracking.assertIdle();
          record(p, c, s, t, d, n);
        }));
    for (JdbcMetaDataProgress.Phase p : List.of(PRIMARY_KEYS, FOREIGN_KEYS)) {
      assertEquals(List.of(0, 1, 2),
          phase(p).stream().map(Event::done).collect(Collectors.toList()));
      assertEquals(List.of("CUSTOMERS", "ORDERS"),
          phase(p).stream().skip(1).map(Event::table).collect(Collectors.toList()));
      assertTrue(phase(p).stream().allMatch(e -> e.total == 2));
    }
    conn.rollback();
  }

  @Test
  void emptySchemaWideKeysCompleteWithoutPerTableFallback() throws SQLException {
    Tracking tracking = new Tracking();
    tracking.rejectBulkKeys = true;
    tracking.emptySchemaKeys = true;
    new JdbcMetaData(tracking.connection(), List.of("STAGE"),
        JdbcMetaDataOptions.defaults().setProgress(this::record));
    for (JdbcMetaDataProgress.Phase p : List.of(PRIMARY_KEYS, FOREIGN_KEYS)) {
      assertEquals(List.of(0, 1), phase(p).stream().map(Event::done).collect(Collectors.toList()));
      assertTrue(phase(p).stream().allMatch(e -> e.table == null));
    }
    assertEquals(0, tracking.tableKeyCalls);
  }

  @Test
  void listenerDoesNotChangeMetadataOrJdbcCallsAndOptionsRetainIdentity() throws SQLException {
    JdbcMetaDataProgress listener = this::record;
    assertNull(JdbcMetaDataOptions.defaults().getProgress());
    assertNull(JdbcMetaDataOptions.none().getProgress());
    JdbcMetaDataOptions options = JdbcMetaDataOptions.defaults().withProgress(listener);
    assertSame(listener, options.getProgress());
    assertSame(listener, options.copy().getProgress());
    Tracking plain = new Tracking();
    Tracking observed = new Tracking();
    JdbcMetaData baseline = new JdbcMetaData(plain.connection(), List.of("SALES", "STAGE"),
        JdbcMetaDataOptions.defaults());
    JdbcMetaData withListener =
        new JdbcMetaData(observed.connection(), List.of("SALES", "STAGE"), options);
    assertTrue(
        JdbcJSONSerializer.toJson(baseline).similar(JdbcJSONSerializer.toJson(withListener)));
    assertEquals(plain.calls, observed.calls);
    assertNotNull(options.setProgress(null));
    assertNull(options.getProgress());
    assertTrue(JdbcJSONSerializer.toJson(new JdbcMetaData(conn, List.of("SALES")))
        .similar(JdbcJSONSerializer
            .toJson(new JdbcMetaData(conn, List.of("SALES"), JdbcMetaDataOptions.none()))));
  }

  /** Tracks actual JDBC resource lifetimes and savepoint boundaries, including failed probes. */
  private class Tracking {
    private int resources;
    private int probes;
    private int tableKeyCalls;
    private boolean rejectBulkKeys;
    private boolean rejectSchemaKeys;
    private boolean emptySchemaKeys;
    private Connection connection;
    private final Map<String, Integer> calls = new HashMap<>();

    void assertIdle() {
      assertEquals(0, resources, "Open JDBC resources at callback");
      assertEquals(0, probes, "Active savepoint probe at callback");
    }

    Connection connection() {
      connection = wrap(conn, Connection.class);
      return connection;
    }

    private Object invoke(Object target, Method method, Object[] args) throws Throwable {
      try {
        return method.invoke(target, args);
      } catch (InvocationTargetException ex) {
        throw ex.getCause();
      }
    }

    private <T> T wrap(T target, Class<T> api) {
      boolean resource = target instanceof Statement || target instanceof ResultSet;
      if (resource) {
        resources++;
      }
      boolean[] closed = {false};
      return api.cast(Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {api},
          (proxy, method, args) -> {
            String name = method.getName();
            calls.merge(api.getSimpleName() + "." + name, 1, Integer::sum);
            if (target instanceof DatabaseMetaData && name.equals("getConnection")) {
              return connection;
            }
            if (target instanceof Connection && name.equals("prepareStatement") && rejectBulkKeys) {
              String sql = ((String) args[0]).toLowerCase(java.util.Locale.ROOT);
              if (sql.contains("table_constraints") || sql.contains("key_column_usage")
                  || sql.contains("referential_constraints")) {
                throw new SQLFeatureNotSupportedException("No bulk keys");
              }
            }
            if (target instanceof DatabaseMetaData
                && (name.equals("getPrimaryKeys") || name.equals("getImportedKeys"))) {
              if (args[2] == null && rejectSchemaKeys) {
                throw new SQLFeatureNotSupportedException("No null table");
              }
              if (args[2] != null) {
                tableKeyCalls++;
              } else if (emptySchemaKeys) {
                // H2 rejects null tables; emulate a driver accepting schema-wide empty results.
                args = args.clone();
                args[2] = "MISSING_TABLE";
              }
            }
            Object result = invoke(target, method, args);
            if (name.equals("setSavepoint")) {
              probes++;
            } else if (name.equals("releaseSavepoint")) {
              probes--;
            } else if (name.equals("close") && resource && !closed[0]) {
              closed[0] = true;
              resources--;
            }
            if (result instanceof DatabaseMetaData) {
              return wrap((DatabaseMetaData) result, DatabaseMetaData.class);
            }
            if (result instanceof PreparedStatement) {
              return wrap((PreparedStatement) result, PreparedStatement.class);
            }
            if (result instanceof Statement) {
              return wrap((Statement) result, Statement.class);
            }
            if (result instanceof ResultSet) {
              return wrap((ResultSet) result, ResultSet.class);
            }
            return result;
          }));
    }
  }
}
