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

import ai.starlake.transpiler.schema.JdbcUtils.DatabaseSpecific;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CatalogDiscoveryTest {
  @ParameterizedTest
  @EnumSource(value = DatabaseSpecific.class, names = {"SNOWFLAKE", "DATABRICKS"})
  void discoversEveryCatalogAndPreservesNativeDetails(DatabaseSpecific type) throws Exception {
    try (Fixture f = new Fixture(type)) {
      JdbcMetaData md = new JdbcMetaData(f.connection, List.of(),
          JdbcMetaDataOptions.none().setColumnDetails(true));
      for (String catalog : List.of("A", "B")) {
        JdbcTable table = md.get(catalog).get("PUBLIC").get("T");
        assertNotNull(table);
        assertEquals(catalog, table.columns.get("ID").tableCatalog);
        assertEquals(catalog, table.columns.get("ID").scopeCatalog);
        assertEquals("PUBLIC", table.columns.get("ID").scopeSchema);
        assertEquals("T", table.columns.get("ID").scopeTable);
        assertEquals("ID", table.columns.get("ID").scopeColumn);
        assertEquals("YES", table.columns.get("ID").isAutomaticIncrement);
        assertEquals("YES", table.columns.get("COMPUTED").isGeneratedColumn);
        assertTrue(table.columns.get("LABEL").columnDefinition.contains("pending"));
        assertEquals("native comment", table.columns.get("LABEL").remarks);
        assertEquals(12, table.columns.get("AMOUNT").columnSize);
        assertEquals(3, table.columns.get("AMOUNT").decimalDigits);
      }
      assertEquals(List.of("A", "B"), f.scopedCatalogs);
      assertEquals(List.of("A.PUBLIC", "B.PUBLIC"), f.tableScopes);
      assertEquals(f.tableScopes, f.columnScopes);
      assertEquals("A", f.connection.getCatalog());
      assertEquals("PUBLIC", f.connection.getSchema());
      assertEquals(0, f.genericQueries);
    }
  }

  @ParameterizedTest
  @EnumSource(value = DatabaseSpecific.class, names = {"SNOWFLAKE", "DATABRICKS"})
  void enrichmentKeepsCatalogOwnershipAndComments(DatabaseSpecific type) throws Exception {
    try (Fixture f = new Fixture(type)) {
      f.allowEnrichment = true;
      JdbcMetaData md = new JdbcMetaData(f.connection, List.of(), JdbcMetaDataOptions.defaults());
      for (String catalog : List.of("A", "B")) {
        JdbcTable table = md.get(catalog).get("PUBLIC").get("T");
        assertEquals(List.of("ID"), table.primaryKey.columnNames);
        assertEquals(catalog, table.primaryKey.tableCatalog);
        assertEquals(1, table.foreignKeys.size());
        assertEquals(catalog, table.foreignKeys.get(0).pkTableCatalog);
        assertEquals("native table comment", table.remarks);
        assertEquals("native comment", table.columns.get("LABEL").remarks);
        assertEquals("YES", table.columns.get("ID").isAutomaticIncrement);
        assertEquals("YES", table.columns.get("COMPUTED").isGeneratedColumn);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(value = DatabaseSpecific.class, names = {"SNOWFLAKE", "DATABRICKS"})
  void filtersCatalogsAndSchemaWildcards(DatabaseSpecific type) throws Exception {
    try (Fixture f = new Fixture(type)) {
      JdbcMetaData md = new JdbcMetaData(f.connection, List.of("B.PUB%", "b.PUBLIC"));
      assertNotNull(md.get("B").get("PUBLIC").get("T"));
      assertNull(md.get("A"));
      assertEquals(List.of("B.PUBLIC"), f.tableScopes);
      assertEquals(f.tableScopes, f.columnScopes);
    }
    try (Fixture f = new Fixture(type)) {
      JdbcMetaData md = new JdbcMetaData(f.connection, List.of("LEGACY.PUBLIC"));
      assertNotNull(md.get("LEGACY").get("PUBLIC").get("T"));
      assertEquals(List.of("LEGACY"), f.scopedCatalogs);
    }
    try (Fixture f = new Fixture(type)) {
      f.omitCurrentCatalog = true;
      JdbcMetaData md = new JdbcMetaData(f.connection, List.of("PUBLIC"));
      assertNotNull(md.get("A").get("PUBLIC").get("T"));
      assertNull(md.get("B"));
      assertEquals(List.of("A"), f.scopedCatalogs);
    }
  }

  @ParameterizedTest
  @EnumSource(value = DatabaseSpecific.class, names = {"SNOWFLAKE", "DATABRICKS"})
  void unscopedFallbackRunsOnceAndUsesReportedCatalogs(DatabaseSpecific type) throws Exception {
    try (Fixture f = new Fixture(type)) {
      f.unsupportedScoped = true;
      JdbcMetaData md = new JdbcMetaData(f.connection);
      assertNotNull(md.get("A").get("PUBLIC").get("T"));
      assertNotNull(md.get("B").get("PUBLIC").get("T"));
      assertEquals(1, f.unscopedReads);
    }
    try (Fixture f = new Fixture(type)) {
      f.unsupportedScoped = true;
      f.missingUnscopedCatalog = true;
      SQLException error = assertThrows(SQLException.class, () -> new JdbcMetaData(f.connection));
      assertTrue(error.getMessage().contains("ambiguous catalog"));
      assertTrue(f.tableScopes.isEmpty());
    }
    try (Fixture f = new Fixture(type)) {
      f.unsupportedScoped = true;
      f.missingUnscopedCatalog = true;
      assertEquals("A", JdbcSchema.getSchemas(f.connection.getMetaData(), List.of("A")).iterator()
          .next().tableCatalog);
      assertEquals(1, f.unscopedReads);
    }
  }

  @ParameterizedTest
  @EnumSource(value = DatabaseSpecific.class, names = {"SNOWFLAKE", "DATABRICKS"})
  void ordinaryDiscoveryFailuresPropagate(DatabaseSpecific type) throws Exception {
    for (String failure : List.of("catalogs", "schemas", "columns")) {
      try (Fixture f = new Fixture(type)) {
        f.failure = failure;
        SQLException error = assertThrows(SQLException.class, () -> new JdbcMetaData(f.connection));
        assertEquals("Failed " + failure, error.getMessage());
        assertEquals(0, f.unscopedReads);
        assertEquals(1, f.catalogReads);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(value = DatabaseSpecific.class, names = {"SNOWFLAKE", "DATABRICKS"})
  void skipsExcludedSchemas(DatabaseSpecific type) throws Exception {
    try (Fixture f = new Fixture(type)) {
      f.systemSchema = true;
      JdbcMetaData md = new JdbcMetaData(f.connection);
      assertNotNull(md.get("A").get("PUBLIC").get("T"));
      if (type == DatabaseSpecific.DATABRICKS) {
        assertFalse(f.tableScopes.contains("A.INFORMATION_SCHEMA"));
      }
    }
  }

  @Test
  void discoveryPreservesCallerTransaction() throws Exception {
    try (Fixture f = new Fixture(DatabaseSpecific.SNOWFLAKE)) {
      f.base.setAutoCommit(false);
      f.base.createStatement().execute("INSERT INTO T(LABEL) VALUES ('caller')");
      java.sql.Savepoint marker = f.base.setSavepoint();
      new JdbcMetaData(f.connection);
      assertFalse(f.connection.getAutoCommit());
      f.base.rollback(marker);
      try (ResultSet rs = f.base.createStatement().executeQuery("SELECT COUNT(*) FROM T")) {
        if (!rs.next()) {
          fail("Expected a row count after metadata discovery");
        }
        assertEquals(1, rs.getInt(1));
      }
      f.base.rollback();
    }
  }

  @Test
  void exactCatalogRequestsOnlyMatchingScopedApi() throws Exception {
    try (Fixture f = new Fixture(DatabaseSpecific.SNOWFLAKE)) {
      new JdbcMetaData(f.connection, List.of("B.PUB%"));
      assertEquals(List.of("B:PUB%"), f.schemaRequests);
    }
  }

  @Test
  void allCatalogOptionRestoresCataloglessFilterBehavior() throws Exception {
    try (Fixture f = new Fixture(DatabaseSpecific.SNOWFLAKE)) {
      JdbcMetaData md = new JdbcMetaData(f.connection, List.of("PUBLIC"),
          JdbcMetaDataOptions.none().setAllCatalogs(true));
      assertNotNull(md.get("A").get("PUBLIC").get("T"));
      assertNotNull(md.get("B").get("PUBLIC").get("T"));
      assertEquals(List.of("A:PUBLIC", "B:PUBLIC"), f.schemaRequests);
    }
  }

  @Test
  void unreadableVisibleCatalogIsSkippedIncludingExplicitFilters() throws Exception {
    try (Fixture f = new Fixture(DatabaseSpecific.SNOWFLAKE)) {
      f.unreadableCatalog = true;
      assertNotNull(new JdbcMetaData(f.connection).get("A").get("PUBLIC").get("T"));
      assertNull(new JdbcMetaData(f.connection, List.of("B.PUBLIC")).get("B"));
    }
  }

  @Test
  void snowflakeKeysHaveConstantCostIncludingEmptyAnswers() throws Exception {
    for (int count : List.of(1, 20)) {
      for (boolean empty : List.of(true, false)) {
        try (Fixture f = new Fixture(DatabaseSpecific.SNOWFLAKE)) {
          f.allowEnrichment = true;
          f.emptyKeys = empty;
          try (Statement st = f.base.createStatement()) {
            for (int i = 1; i < count; i++) {
              st.execute("CREATE TABLE T" + i + "(ID INT)");
            }
          }
          JdbcMetaData md =
              new JdbcMetaData(f.connection, List.of("A.PUBLIC"), JdbcMetaDataOptions.defaults());
          assertEquals(count, md.get("A").get("PUBLIC").tables.size());
          assertEquals(List.of("SHOW PRIMARY KEYS IN SCHEMA \"A\".\"PUBLIC\"",
              "SHOW IMPORTED KEYS IN SCHEMA \"A\".\"PUBLIC\""), f.showRequests);
          assertTrue(f.keyRequests.isEmpty());
        }
      }
    }
  }

  @Test
  void snowflakeShowFailureTriesSchemaWideJdbcBeforePerTable() throws Exception {
    for (boolean rejectNull : List.of(false, true)) {
      try (Fixture f = new Fixture(DatabaseSpecific.SNOWFLAKE)) {
        f.allowEnrichment = true;
        f.rejectShow = true;
        f.rejectNullTable = rejectNull;
        new JdbcMetaData(f.connection, List.of("A.PUBLIC"), JdbcMetaDataOptions.defaults());
        assertEquals(
            rejectNull ? List.of("getPrimaryKeys:null", "getPrimaryKeys:T", "getImportedKeys:null",
                "getImportedKeys:T") : List.of("getPrimaryKeys:null", "getImportedKeys:null"),
            f.keyRequests);
      }
    }
  }

  @Test
  void jdbcStrategyRejectingNullTableUsesPerTableFallback() throws Exception {
    try (Fixture f = new Fixture(DatabaseSpecific.DATABRICKS)) {
      f.allowEnrichment = true;
      f.rejectNullTable = true;
      new JdbcMetaData(f.connection, List.of("A.PUBLIC"), JdbcMetaDataOptions.defaults());
      assertEquals(List.of("getPrimaryKeys:null", "getPrimaryKeys:T", "getImportedKeys:null",
          "getImportedKeys:T"), f.keyRequests);
    }
  }

  @Test
  void snowflakeQuotesCatalogAndSchemaAndAcceptsEmptyJdbcFallback() throws Exception {
    try (Fixture f = new Fixture(DatabaseSpecific.SNOWFLAKE)) {
      f.emptyKeys = true;
      JdbcMetaData md = new JdbcMetaData("B\"Q", "Mixed Schema");
      md.addTable("T", new JdbcColumn("ID"));
      JdbcKeyExtractor.enrich(f.connection, md,
          JdbcMetaDataOptions.defaults().setComments(false).setColumnDetails(false));
      assertEquals(List.of("SHOW PRIMARY KEYS IN SCHEMA \"B\"\"Q\".\"Mixed Schema\"",
          "SHOW IMPORTED KEYS IN SCHEMA \"B\"\"Q\".\"Mixed Schema\""), f.showRequests);
      assertTrue(f.keyRequests.isEmpty());
    }
    try (Fixture f = new Fixture(DatabaseSpecific.SNOWFLAKE)) {
      f.allowEnrichment = true;
      f.rejectShow = true;
      f.emptyKeys = true;
      new JdbcMetaData(f.connection, List.of("A.PUBLIC"), JdbcMetaDataOptions.defaults());
      assertEquals(List.of("getPrimaryKeys:null", "getImportedKeys:null"), f.keyRequests);
    }
  }

  @Test
  void failedShowRecoveryNeverAttemptsJdbcFallback() throws Exception {
    try (Fixture f = new Fixture(DatabaseSpecific.SNOWFLAKE)) {
      f.base.setAutoCommit(false);
      f.rejectShow = true;
      f.failRecovery = true;
      assertThrows(JdbcUtils.MetadataRecoveryException.class, () -> new JdbcMetaData(f.connection,
          List.of("A.PUBLIC"), JdbcMetaDataOptions.defaults()));
      assertTrue(f.keyRequests.isEmpty());
    }
  }

  private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException ex) {
      throw ex.getCause();
    }
  }

  private static final class Fixture implements AutoCloseable {
    final Connection base = DriverManager.getConnection("jdbc:h2:mem:");
    final Connection connection;
    final List<String> scopedCatalogs = new ArrayList<>();
    final List<String> tableScopes = new ArrayList<>();
    final List<String> columnScopes = new ArrayList<>();
    final List<String> schemaRequests = new ArrayList<>();
    final List<String> keyRequests = new ArrayList<>();
    final List<String> showRequests = new ArrayList<>();
    boolean failRecovery;
    boolean rejectShow;
    boolean rejectNullTable;
    boolean emptyKeys;
    boolean unreadableCatalog;
    boolean unsupportedScoped;
    boolean missingUnscopedCatalog;
    boolean omitCurrentCatalog;
    boolean systemSchema;
    boolean allowEnrichment;
    String failure;
    int unscopedReads;
    int genericQueries;
    int catalogReads;

    Fixture(DatabaseSpecific type) throws SQLException {
      try (Statement st = base.createStatement()) {
        st.execute("CREATE TABLE T(ID INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, "
            + "COMPUTED INTEGER GENERATED ALWAYS AS (ID+1), LABEL VARCHAR(20) DEFAULT 'pending', "
            + "AMOUNT DECIMAL(12,3), LINK INTEGER REFERENCES T(ID))");
        st.execute("COMMENT ON TABLE T IS 'native table comment'");
        st.execute("COMMENT ON COLUMN T.LABEL IS 'native comment'");
      }
      Connection[] wrapped = new Connection[1];
      DatabaseMetaData delegate = base.getMetaData();
      DatabaseMetaData md = (DatabaseMetaData) Proxy.newProxyInstance(getClass().getClassLoader(),
          new Class<?>[] {DatabaseMetaData.class}, (proxy, method, args) -> {
            String name = method.getName();
            if (name.equals("getDatabaseProductName")) {
              return type.name();
            }
            if (name.equals("getConnection")) {
              return wrapped[0];
            }
            if (name.equals("getCatalogs")) {
              catalogReads++;
              if ("catalogs".equals(failure)) {
                throw new SQLException("Failed catalogs");
              }
              return rows(omitCurrentCatalog ? "SELECT 'B' AS TABLE_CAT"
                  : "SELECT 'A' AS TABLE_CAT UNION ALL SELECT 'B'");
            }
            if (name.equals("getSchemas")) {
              if (args != null && args.length == 2) {
                scopedCatalogs.add((String) args[0]);
                schemaRequests.add(args[0] + ":" + args[1]);
                if (unreadableCatalog && "B".equals(args[0])) {
                  throw new SQLException("Insufficient privilege", "42501");
                }
                if (unsupportedScoped) {
                  throw new SQLFeatureNotSupportedException("Scoped schemas unsupported");
                }
                if ("schemas".equals(failure) && "B".equals(args[0])) {
                  throw new SQLException("Failed schemas");
                }
                return rows("SELECT 'PUBLIC' AS TABLE_SCHEM, CAST(NULL AS VARCHAR) AS TABLE_CATALOG"
                    + (systemSchema ? " UNION ALL SELECT 'INFORMATION_SCHEMA', NULL" : ""));
              }
              unscopedReads++;
              return rows(missingUnscopedCatalog
                  ? "SELECT 'PUBLIC' AS TABLE_SCHEM, CAST(NULL AS VARCHAR) AS TABLE_CATALOG"
                  : "SELECT 'PUBLIC' AS TABLE_SCHEM, 'A' AS TABLE_CATALOG UNION ALL SELECT 'PUBLIC', 'B'");
            }
            if (name.equals("getPrimaryKeys") || name.equals("getImportedKeys")) {
              keyRequests.add(name + ":" + args[2]);
              if (rejectNullTable && args[2] == null) {
                throw new SQLException("Table required");
              }
              if (emptyKeys) {
                return rows("SELECT 1 WHERE FALSE");
              }
            }
            if (name.equals("getPrimaryKeys")) {
              return scopedRows(delegate.getPrimaryKeys(null, "PUBLIC", "T"), (String) args[0]);
            }
            if (name.equals("getImportedKeys")) {
              return scopedRows(delegate.getImportedKeys(null, "PUBLIC", "T"), (String) args[0]);
            }
            if (name.equals("getTables") || name.equals("getColumns")) {
              assertNotNull(args[0], "Each discovery read must name its catalog");
              String catalog = (String) args[0];
              String schema = (String) args[1];
              if ("columns".equals(failure) && name.equals("getColumns") && catalog.equals("B")) {
                throw new SQLException("Failed columns");
              }
              (name.equals("getTables") ? tableScopes : columnScopes).add(catalog + "." + schema);
              ResultSet rs = name.equals("getTables")
                  ? delegate.getTables(null, schema, "%", (String[]) args[3])
                  : delegate.getColumns(null, schema, "%", "%");
              return Proxy.newProxyInstance(getClass().getClassLoader(),
                  new Class<?>[] {ResultSet.class}, (p, m, a) -> {
                    if (m.getName().equals("getString") && "TABLE_CAT".equals(a[0])) {
                      return catalog;
                    }
                    return invoke(rs, m, a);
                  });
            }
            return invoke(delegate, method, args);
          });
      wrapped[0] = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
          new Class<?>[] {Connection.class}, (proxy, method, args) -> {
            String name = method.getName();
            if (name.equals("getMetaData")) {
              return md;
            }
            if (name.equals("getCatalog")) {
              return "A";
            }
            if (name.equals("getSchema")) {
              return "PUBLIC";
            }
            if (failRecovery && name.equals("rollback")) {
              throw new SQLException("Recovery failed");
            }
            if (name.equals("setCatalog") || name.equals("setSchema") || name.equals("commit")
                || name.equals("setAutoCommit") || name.equals("rollback") && args.length == 0) {
              throw new AssertionError("Scanner changed caller state: " + name);
            }
            if (name.equals("prepareStatement")) {
              genericQueries++;
              assertTrue(allowEnrichment, "Generic SQL used for JDBC discovery");
              String query = (String) args[0];
              if (type == DatabaseSpecific.SNOWFLAKE) {
                assertFalse(query.contains("key_column_usage"));
              }
              String catalog = query.contains("\"A\".") ? "A" : "B";
              PreparedStatement statement = base.prepareStatement("SELECT 1");
              return Proxy.newProxyInstance(getClass().getClassLoader(),
                  new Class<?>[] {PreparedStatement.class}, (p, m, a) -> {
                    if (m.getName().equals("setString")) {
                      return null;
                    }
                    if (m.getName().equals("executeQuery")) {
                      if (query.contains("referential_constraints")) {
                        return scopedRows(delegate.getImportedKeys(null, "PUBLIC", "T"), catalog);
                      }
                      if (query.contains("table_constraints")) {
                        return delegate.getPrimaryKeys(null, "PUBLIC", "T");
                      }
                      assertTrue(query.contains("COMMENT_TEXT"));
                      return rows("SELECT 'T' AS TABLE_NAME, CAST(NULL AS VARCHAR) AS COLUMN_NAME, "
                          + "'native table comment' AS COMMENT_TEXT UNION ALL "
                          + "SELECT 'T', 'LABEL', 'native comment'");
                    }
                    return invoke(statement, m, a);
                  });
            }
            if (name.equals("createStatement")) {
              Statement st = base.createStatement();
              return Proxy.newProxyInstance(getClass().getClassLoader(),
                  new Class<?>[] {Statement.class}, (p, m, a) -> {
                    if (m.getName().equals("executeQuery")) {
                      String sql = (String) a[0];
                      if (sql.startsWith("SHOW ")) {
                        showRequests.add(sql);
                        if (rejectShow) {
                          throw new SQLException("SHOW denied", "42501");
                        }
                        String catalog = sql.contains("\"A\".") ? "A" : "B";
                        boolean primary = sql.startsWith("SHOW PRIMARY");
                        ResultSet keys = primary ? delegate.getPrimaryKeys(null, "PUBLIC", "T")
                            : delegate.getImportedKeys(null, "PUBLIC", "T");
                        keys = scopedRows(keys, catalog);
                        final ResultSet source = keys;
                        java.util.Map<String, String> labels =
                            java.util.Map.ofEntries(java.util.Map.entry("table_name", "TABLE_NAME"),
                                java.util.Map.entry("column_name", "COLUMN_NAME"),
                                java.util.Map.entry("constraint_name", "PK_NAME"),
                                java.util.Map.entry("key_sequence", "KEY_SEQ"),
                                java.util.Map.entry("fk_table_name", "FKTABLE_NAME"),
                                java.util.Map.entry("fk_column_name", "FKCOLUMN_NAME"),
                                java.util.Map.entry("fk_database_name", "FKTABLE_CAT"),
                                java.util.Map.entry("fk_schema_name", "FKTABLE_SCHEM"),
                                java.util.Map.entry("pk_table_name", "PKTABLE_NAME"),
                                java.util.Map.entry("pk_column_name", "PKCOLUMN_NAME"),
                                java.util.Map.entry("pk_database_name", "PKTABLE_CAT"),
                                java.util.Map.entry("pk_schema_name", "PKTABLE_SCHEM"));
                        return Proxy.newProxyInstance(getClass().getClassLoader(),
                            new Class<?>[] {ResultSet.class}, (keyProxy, keyMethod, keyArgs) -> {
                              if (keyMethod.getName().equals("next") && emptyKeys) {
                                return false;
                              }
                              if (keyArgs != null && keyArgs.length > 0
                                  && keyArgs[0] instanceof String) {
                                String label = (String) keyArgs[0];
                                keyArgs[0] = labels.getOrDefault(label,
                                    label.toUpperCase(java.util.Locale.ROOT));
                              }
                              return invoke(source, keyMethod, keyArgs);
                            });
                      }
                      assertEquals(type.getCurrentSchemaQuery(), a[0]);
                      return st.executeQuery("SELECT 'A', 'PUBLIC'");
                    }
                    return invoke(st, m, a);
                  });
            }
            return invoke(base, method, args);
          });
      connection = wrapped[0];
    }

    private ResultSet scopedRows(ResultSet rs, String catalog) {
      return (ResultSet) Proxy.newProxyInstance(getClass().getClassLoader(),
          new Class<?>[] {ResultSet.class}, (p, m, a) -> {
            if (m.getName().equals("getString")
                && List.of("TABLE_CAT", "PKTABLE_CAT", "FKTABLE_CAT").contains(a[0])) {
              return catalog;
            }
            return invoke(rs, m, a);
          });
    }

    private ResultSet rows(String sql) throws SQLException {
      Statement statement = base.createStatement();
      statement.closeOnCompletion();
      return statement.executeQuery(sql);
    }

    @Override
    public void close() throws SQLException {
      base.close();
    }
  }
}
