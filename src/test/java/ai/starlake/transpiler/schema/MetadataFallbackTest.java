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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Savepoint;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MetadataFallbackTest {
  @ParameterizedTest
  @EnumSource(DatabaseSpecific.class)
  void profilesRecoverProbesAndMissingScopes(DatabaseSpecific type) throws SQLException {
    for (boolean savepoints : List.of(true, false)) {
      try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:")) {
        try (Statement st = conn.createStatement()) {
          st.execute("CREATE TABLE PUBLIC.SAMPLE (ID INTEGER)");
        }
        conn.setAutoCommit(false);
        Savepoint marker = conn.setSavepoint();
        try (Statement st = conn.createStatement()) {
          st.execute("INSERT INTO PUBLIC.SAMPLE VALUES (22)");
        }
        List<String> calls = new ArrayList<>();
        Connection profile = profileConnection(conn, type, savepoints, calls);
        JdbcMetaData metadata = new JdbcMetaData(profile, List.of("PUBLIC"));
        JdbcTable table = metadata.get(conn.getCatalog()).get("PUBLIC").get("SAMPLE");
        assertNotNull(table);
        assertEquals(1, table.columns.size(), type + " must attach null-catalog columns");
        assertEquals(conn.getCatalog(), table.columns.get("ID").tableCatalog);
        assertEquals("PUBLIC", table.columns.get("ID").tableSchema);
        assertFalse(profile.getAutoCommit());
        if (savepoints && !type.usesJdbcMetadata()) {
          assertTrue(calls.contains("rollback-savepoint"));
        } else if (!savepoints) {
          assertFalse(calls.contains("probe"), "No speculative SQL without savepoints");
        }
        try (Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("SELECT ID FROM SAMPLE")) {
          if (!rs.next()) {
            fail("Metadata scan discarded caller work");
          }
          assertEquals(22, rs.getInt(1));
        }
        conn.rollback(marker);
        conn.rollback();
      }
    }
  }

  @Test
  void recoveryFailureStopsBeforeFallback() throws SQLException {
    try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:")) {
      conn.setAutoCommit(false);
      Connection broken = proxy(Connection.class, conn, (method, args) -> {
        if (method.equals("rollback")) {
          throw new SQLException("Forced savepoint rollback failure");
        }
        return UNHANDLED;
      });
      assertThrows(JdbcUtils.MetadataRecoveryException.class,
          () -> JdbcUtils.metadataProbe(broken, () -> {
            throw new SQLException("Forced probe failure");
          }));
      conn.rollback();
    }
  }

  @Test
  void unsupportedSavepointReleasePreservesSuccessAndFailedProbeRecovery() throws SQLException {
    try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:")) {
      conn.setAutoCommit(false);
      Savepoint caller = conn.setSavepoint();
      Connection noRelease = proxy(Connection.class, conn, (method, args) -> {
        if (method.equals("releaseSavepoint")) {
          throw new java.sql.SQLFeatureNotSupportedException("Release is unsupported");
        }
        return UNHANDLED;
      });
      assertEquals("result", JdbcUtils.metadataProbe(noRelease, () -> "result"));
      SQLException original = new SQLException("Probe failure");
      assertSame(original,
          assertThrows(SQLException.class, () -> JdbcUtils.metadataProbe(noRelease, () -> {
            throw original;
          })));
      conn.rollback(caller);
      conn.rollback();
    }
  }

  @Test
  void realSavepointReleaseFailureStillStopsScan() throws SQLException {
    try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:")) {
      conn.setAutoCommit(false);
      Connection broken = proxy(Connection.class, conn, (method, args) -> {
        if (method.equals("releaseSavepoint")) {
          throw new SQLException("Release failed");
        }
        return UNHANDLED;
      });
      assertThrows(JdbcUtils.MetadataRecoveryException.class,
          () -> JdbcUtils.metadataProbe(broken, () -> "result"));
      conn.rollback();
    }
  }

  @Test
  void jsonDoesNotInventCatalogFromCurrentDatabase() {
    JdbcMetaData metadata = new JdbcMetaData("FREEPDB1", "TEST");
    metadata.clear();
    JdbcCatalog catalog = new JdbcCatalog("", ".");
    catalog.put(new JdbcSchema("TEST", ""));
    metadata.put(catalog);
    String json = JdbcJSONSerializer.toJson(metadata).toString();
    JdbcMetaData restored = JdbcJSONSerializer.fromJson(new java.io.StringReader(json));
    assertEquals(1, restored.getCatalogsList().size());
    assertEquals("", restored.getCatalogsList().get(0).tableCatalog);
    assertEquals("FREEPDB1", restored.getCurrentCatalogName());
    assertEquals(json, JdbcJSONSerializer.toJson(restored).toString());
  }

  @Test
  void schemaLessMysqlFiltersCatalogsAndResolvesUnqualifiedTables() throws Exception {
    try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:")) {
      try (Statement st = conn.createStatement()) {
        st.execute("CREATE TABLE PUBLIC.T(A INT)");
      }
      Connection[] wrapped = new Connection[1];
      DatabaseMetaData delegate = conn.getMetaData();
      DatabaseMetaData md = proxy(DatabaseMetaData.class, delegate, (method, args) -> {
        if (method.equals("getDatabaseProductName")) {
          return "MySQL";
        }
        if (method.equals("getConnection")) {
          return wrapped[0];
        }
        if (method.startsWith("supportsSchemas")) {
          return false;
        }
        if (method.equals("getTables") || method.equals("getColumns")) {
          ResultSet rows =
              method.equals("getTables") ? delegate.getTables((String) args[0], "PUBLIC", "%", null)
                  : delegate.getColumns((String) args[0], "PUBLIC", "%", "%");
          return proxy(ResultSet.class, rows, (name, parameters) -> {
            if (name.equals("getString") && "TABLE_SCHEM".equals(parameters[0])) {
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
        if (method.equals("prepareStatement")) {
          fail("Base extraction must use JDBC's catalog/schema mapping");
        }
        if (method.equals("createStatement")) {
          Statement st = conn.createStatement();
          return proxy(Statement.class, st, (name, parameters) -> {
            if (name.equals("executeQuery")) {
              assertEquals(DatabaseSpecific.MYSQL.getCurrentSchemaQuery(), parameters[0]);
              return st.executeQuery("SELECT current_catalog(), current_catalog()");
            }
            return UNHANDLED;
          });
        }
        return UNHANDLED;
      });
      for (List<String> patterns : List.of(List.<String>of(),
          List.of(conn.getCatalog().toLowerCase(java.util.Locale.ROOT)))) {
        JdbcMetaData metadata = new JdbcMetaData(wrapped[0], patterns);
        assertEquals(conn.getCatalog(), metadata.getCurrentCatalogName());
        assertEquals("", metadata.getCurrentSchemaName());
        assertNotNull(metadata.get(conn.getCatalog()).get("").get("T").columns.get("A"));
        assertNotNull(new ai.starlake.transpiler.JSQLColumResolver(metadata).getLineage(
            ai.starlake.transpiler.schema.treebuilder.JSONObjectTreeBuilder.class,
            "select x.a from T x"));
      }
      JdbcMetaData unmatched = new JdbcMetaData(wrapped[0], List.of("MISSING"));
      assertTrue(unmatched.getCatalogsList().stream().flatMap(c -> c.schemas.values().stream())
          .allMatch(schema -> schema.tables.isEmpty()));
    }
  }

  @Test
  void legacyJsonMissingCurrentCatalogUsesEmptyCatalogEvenWithOtherCatalogs() throws Exception {
    JdbcMetaData metadata = new JdbcMetaData("FREEPDB1", "TEST");
    metadata.clear();
    metadata.addTable("", "TEST", "T", List.of(new JdbcColumn("A")));
    metadata.addTable("OTHER", "TEST", "OTHER_T", List.of(new JdbcColumn("B")));
    JdbcMetaData restored = JdbcJSONSerializer
        .fromJson(new java.io.StringReader(JdbcJSONSerializer.toJson(metadata).toString()));
    assertEquals("", restored.getCurrentCatalogName());
    assertNotNull(new ai.starlake.transpiler.JSQLColumResolver(restored).getLineage(
        ai.starlake.transpiler.schema.treebuilder.JSONObjectTreeBuilder.class,
        "select x.a from T x"));
  }

  @Test
  void oracleCatalogContextAndLegacyJsonResolveWithoutInformationSchema() throws Exception {
    for (boolean supportsCatalogs : List.of(false, true)) {
      try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:")) {
        try (Statement st = conn.createStatement()) {
          st.execute("CREATE SCHEMA TEST");
          st.execute("CREATE TABLE TEST.T(A INT PRIMARY KEY)");
        }
        conn.setAutoCommit(false);
        Connection[] wrapped = new Connection[1];
        DatabaseMetaData delegate = conn.getMetaData();
        DatabaseMetaData md = proxy(DatabaseMetaData.class, delegate, (method, args) -> {
          if (method.equals("getDatabaseProductName")) {
            return "Oracle";
          }
          if (method.equals("getConnection")) {
            return wrapped[0];
          }
          if (method.startsWith("supportsCatalogs")) {
            return supportsCatalogs;
          }
          if (method.equals("getSchemas") || method.equals("getTables")
              || method.equals("getColumns")) {
            ResultSet rows = method.equals("getSchemas") ? delegate.getSchemas()
                : method.equals("getTables") ? delegate.getTables(null, "TEST", "%", null)
                    : delegate.getColumns(null, "TEST", "%", "%");
            return proxy(ResultSet.class, rows, (name, parameters) -> {
              if (name.equals("getString")
                  && ("TABLE_CAT".equals(parameters[0]) || "TABLE_CATALOG".equals(parameters[0]))) {
                return null;
              }
              return UNHANDLED;
            });
          }
          return UNHANDLED;
        });
        List<String> sql = new ArrayList<>();
        wrapped[0] = proxy(Connection.class, conn, (method, args) -> {
          if (method.equals("getMetaData")) {
            return md;
          }
          if (method.equals("getCatalog")) {
            return null;
          }
          if (method.equals("createStatement")) {
            Statement st = conn.createStatement();
            return proxy(Statement.class, st, (name, parameters) -> {
              if (name.equals("executeQuery")) {
                sql.add((String) parameters[0]);
                if (parameters[0].equals(DatabaseSpecific.ORACLE.getCurrentSchemaQuery())) {
                  return st.executeQuery("SELECT 'FREEPDB1', 'TEST'");
                }
              }
              return UNHANDLED;
            });
          }
          if (method.equals("prepareStatement")) {
            sql.add((String) args[0]);
          }
          return UNHANDLED;
        });
        List<java.util.logging.LogRecord> warnings = new ArrayList<>();
        java.util.logging.Handler handler = new java.util.logging.Handler() {
          public void publish(java.util.logging.LogRecord record) {
            if (record.getLevel().intValue() >= java.util.logging.Level.WARNING.intValue()) {
              warnings.add(record);
            }
          }

          public void flush() {}

          public void close() {}
        };
        java.util.logging.Logger logger = java.util.logging.Logger.getLogger("");
        logger.addHandler(handler);
        try {
          JdbcMetaData metadata =
              new JdbcMetaData(wrapped[0], List.of("TEST"), JdbcMetaDataOptions.defaults());
          org.json.JSONObject json = JdbcJSONSerializer.toJson(metadata);
          assertEquals("", json.getString("currentCatalog"));
          assertNotNull(new ai.starlake.transpiler.JSQLColumResolver(metadata).getLineage(
              ai.starlake.transpiler.schema.treebuilder.JSONObjectTreeBuilder.class,
              "select x.a from T x"));
          json.put("currentCatalog", "FREEPDB1");
          JdbcMetaData restored =
              JdbcJSONSerializer.fromJson(new java.io.StringReader(json.toString()));
          assertEquals("", restored.getCurrentCatalogName());
          assertNotNull(new ai.starlake.transpiler.JSQLColumResolver(restored).getLineage(
              ai.starlake.transpiler.schema.treebuilder.JSONObjectTreeBuilder.class,
              "select x.a from T x"));
          assertTrue(
              sql.stream().noneMatch(
                  query -> query.toLowerCase(java.util.Locale.ROOT).contains("information_schema")),
              sql.toString());
          assertTrue(warnings.isEmpty(), warnings.toString());
        } finally {
          logger.removeHandler(handler);
          conn.rollback();
        }
      }
    }
  }

  @Test
  void catalogLessDriverDoesNotAcquireConnectionCatalog() throws SQLException {
    try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:")) {
      DatabaseMetaData md = proxy(DatabaseMetaData.class, conn.getMetaData(), (method, args) -> {
        if (method.startsWith("supportsCatalogs") || method.startsWith("supportsSchemas")) {
          return false;
        }
        return UNHANDLED;
      });
      assertEquals("", JdbcUtils.metadataCatalog(md, null));
      assertEquals("", JdbcUtils.metadataSchema(md, null, false));
      assertEquals("EXPLICIT", JdbcUtils.metadataCatalog(md, "EXPLICIT"));
      assertEquals("A_B", JdbcUtils.metadataSchema(md, "A_B", true));
    }
  }

  @Test
  void schemaLessDriversKeepEmptySchemasUnderEachCatalog() throws SQLException {
    try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:")) {
      DatabaseMetaData md = proxy(DatabaseMetaData.class, conn.getMetaData(), (method, args) -> {
        if (method.startsWith("supportsSchemas")) {
          return false;
        }
        if (method.equals("getSchemas")) {
          throw new AssertionError("Driver has no schemas");
        }
        return UNHANDLED;
      });
      java.util.Collection<JdbcSchema> schemas = JdbcSchema.getSchemas(md);
      String catalog = conn.getCatalog();
      assertTrue(schemas.stream()
          .anyMatch(schema -> schema.tableCatalog.equals(catalog) && schema.tableSchema.isEmpty()));
      assertTrue(schemas.stream()
          .anyMatch(schema -> schema.tableCatalog.isEmpty() && schema.tableSchema.isEmpty()));
    }
  }

  @Test
  void informationSchemaRowsUseSameScopeDefaults() throws SQLException {
    try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:")) {
      try (Statement st = conn.createStatement()) {
        st.execute("CREATE TABLE PUBLIC.SAMPLE (ID INTEGER)");
      }
      DatabaseMetaData delegate = conn.getMetaData();
      Connection[] wrapped = new Connection[1];
      DatabaseMetaData md = proxy(DatabaseMetaData.class, delegate, (method, args) -> {
        if (method.equals("getConnection")) {
          return wrapped[0];
        }
        return UNHANDLED;
      });
      wrapped[0] = proxy(Connection.class, conn, (method, args) -> {
        if (method.equals("getMetaData")) {
          return md;
        }
        if (method.equals("prepareStatement")) {
          String sql = ((String) args[0]).toUpperCase(java.util.Locale.ROOT);
          return proxy(PreparedStatement.class, conn.prepareStatement("SELECT 1"),
              (name, parameters) -> {
                if (name.equals("setObject")) {
                  return null;
                }
                if (name.equals("executeQuery")) {
                  // JDBC-shaped rows lack INFORMATION_SCHEMA catalog/schema columns.
                  return sql.contains("INFORMATION_SCHEMA.COLUMNS")
                      ? delegate.getColumns(null, "PUBLIC", "SAMPLE", "%")
                      : delegate.getTables(null, "PUBLIC", "SAMPLE", null);
                }
                return UNHANDLED;
              });
        }
        return UNHANDLED;
      });
      JdbcTable table = JdbcTable.getTables(md, null, null, "%").iterator().next();
      JdbcColumn column = JdbcTable.getColumns(md).iterator().next();
      assertEquals(conn.getCatalog(), table.tableCatalog);
      assertEquals(conn.getCatalog(), column.tableCatalog);
      assertEquals("PUBLIC", table.tableSchema);
      assertEquals("PUBLIC", column.tableSchema);
      assertEquals("SAMPLE", column.tableName);
    }
  }

  @Test
  void failedCurrentContextUsesGettersWithoutDiscardingCallerWork() throws Exception {
    for (boolean autoCommit : List.of(true, false)) {
      try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:")) {
        conn.createStatement().execute("CREATE TABLE T(ID INT)");
        conn.setAutoCommit(autoCommit);
        conn.createStatement().execute("INSERT INTO T VALUES (42)");
        Connection profile = failingContext(conn, null, null);
        JdbcMetaData metadata = new JdbcMetaData(profile);
        assertEquals(conn.getCatalog(), metadata.getCurrentCatalogName());
        assertEquals(conn.getSchema(), metadata.getCurrentSchemaName());
        assertNotNull(new ai.starlake.transpiler.JSQLColumResolver(metadata).getLineage(
            ai.starlake.transpiler.schema.treebuilder.JSONObjectTreeBuilder.class,
            "SELECT ID FROM T"));
        assertEquals(autoCommit, conn.getAutoCommit());
        try (ResultSet rows = conn.createStatement().executeQuery("SELECT ID FROM T")) {
          if (!rows.next()) {
            fail("Metadata fallback discarded caller work");
          }
          assertEquals(42, rows.getInt(1));
        }
        if (!autoCommit) {
          conn.rollback();
        }
      }
    }
  }

  @Test
  void unsupportedContextGettersAreIndependent() throws Exception {
    for (boolean unsupportedCatalog : List.of(true, false)) {
      try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:")) {
        SQLException unsupported = new java.sql.SQLFeatureNotSupportedException("No getter");
        JdbcMetaData metadata = new JdbcMetaData(failingContext(conn,
            unsupportedCatalog ? unsupported : null, unsupportedCatalog ? null : unsupported));
        assertEquals(unsupportedCatalog ? "" : conn.getCatalog(), metadata.getCurrentCatalogName());
        assertEquals(unsupportedCatalog ? conn.getSchema() : "", metadata.getCurrentSchemaName());
      }
    }
  }

  @Test
  void contextGetterFailuresPropagateWithProbeDiagnostic() throws Exception {
    try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:")) {
      SQLException failure = new SQLException("Getter failed");
      SQLException error = assertThrows(SQLException.class,
          () -> new JdbcMetaData(failingContext(conn, failure, null)));
      assertSame(failure, error);
      assertEquals("Current-context SQL failed", error.getSuppressed()[0].getMessage());
    }
  }

  @Test
  void mysqlSchemaModeNormalizesSyntheticCatalogWithoutChangingRealCatalogs() throws Exception {
    try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:")) {
      conn.createStatement().execute("CREATE TABLE T(ID INT)");
      Connection[] wrapped = new Connection[1];
      DatabaseMetaData delegate = conn.getMetaData();
      DatabaseMetaData metadata = proxy(DatabaseMetaData.class, delegate, (name, args) -> {
        if (name.equals("getDatabaseProductName")) {
          return "MySQL";
        }
        if (name.equals("getConnection")) {
          return wrapped[0];
        }
        if (name.startsWith("supportsCatalogs")) {
          return false;
        }
        if (name.equals("getCatalogs")) {
          return proxy(ResultSet.class, delegate.getCatalogs(), (method, parameters) -> {
            if (method.equals("next")) {
              return false;
            }
            return UNHANDLED;
          });
        }
        if (List.of("getSchemas", "getTables", "getColumns").contains(name)) {
          ResultSet rows;
          if (name.equals("getSchemas")) {
            rows = delegate.getSchemas();
          } else if (name.equals("getTables")) {
            rows = delegate.getTables(null, "PUBLIC", "%", null);
          } else {
            rows = delegate.getColumns(null, "PUBLIC", "%", "%");
          }
          return proxy(ResultSet.class, rows, (method, parameters) -> {
            if (method.equals("getString")
                && List.of("TABLE_CAT", "TABLE_CATALOG").contains(parameters[0])) {
              return "def";
            }
            return UNHANDLED;
          });
        }
        return UNHANDLED;
      });
      wrapped[0] = proxy(Connection.class, conn, (name, args) -> {
        if (name.equals("getMetaData")) {
          return metadata;
        }
        if (name.equals("createStatement")) {
          Statement statement = conn.createStatement();
          return proxy(Statement.class, statement,
              (method, parameters) -> method.equals("executeQuery")
                  ? statement.executeQuery("SELECT '', 'PUBLIC'")
                  : UNHANDLED);
        }
        return name.equals("getCatalog") ? null : UNHANDLED;
      });
      JdbcMetaData md = new JdbcMetaData(wrapped[0], List.of("PUBLIC"));
      assertEquals("", md.getCurrentCatalogName());
      assertEquals("PUBLIC", md.getCurrentSchemaName());
      assertNotNull(md.get("").get("PUBLIC").get("T"));
      assertFalse(md.getCatalogMap().containsKey("def"));
      assertNotNull(new ai.starlake.transpiler.JSQLColumResolver(md).getLineage(
          ai.starlake.transpiler.schema.treebuilder.JSONObjectTreeBuilder.class,
          "SELECT T.ID FROM PUBLIC.T"));
      DatabaseMetaData catalogMode = proxy(DatabaseMetaData.class, metadata, (name, args) -> {
        if (name.startsWith("supportsCatalogs")) {
          return true;
        }
        return UNHANDLED;
      });
      assertEquals("def", JdbcUtils.metadataCatalogValue(catalogMode, "def"));
    }
  }

  @Test
  void runtimeFailureInProbeRollsBackAndReleasesSavepoint() throws SQLException {
    try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:")) {
      conn.createStatement().execute("CREATE TABLE T(ID INT)");
      conn.setAutoCommit(false);
      conn.createStatement().execute("INSERT INTO T VALUES (1)");
      List<String> calls = new ArrayList<>();
      Connection tracked = proxy(Connection.class, conn, (method, args) -> {
        if (method.equals("rollback") || method.equals("releaseSavepoint")) {
          calls.add(method);
        }
        return UNHANDLED;
      });
      IllegalStateException failure = new IllegalStateException("Probe bug");
      assertSame(failure,
          assertThrows(IllegalStateException.class, () -> JdbcUtils.metadataProbe(tracked, () -> {
            conn.createStatement().execute("INSERT INTO T VALUES (2)");
            throw failure;
          })));
      assertEquals(List.of("rollback", "releaseSavepoint"), calls);
      try (ResultSet rows = conn.createStatement().executeQuery("SELECT ID FROM T ORDER BY ID")) {
        assertTrue(rows.next(), "Caller work must survive the failed probe");
        assertEquals(1, rows.getInt(1));
        assertFalse(rows.next(), "Partial probe work must be rolled back");
      }
      conn.rollback();
    }
  }

  @Test
  void legacyDriverWithoutGetSchemaFallsBackToEmptySchema() throws Exception {
    try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:")) {
      Connection legacy = failingContext(conn, null, null);
      Connection[] wrapped = new Connection[1];
      DatabaseMetaData md = proxy(DatabaseMetaData.class, legacy.getMetaData(),
          (method, args) -> method.equals("getConnection") ? wrapped[0] : UNHANDLED);
      wrapped[0] = proxy(Connection.class, legacy, (method, args) -> {
        if (method.equals("getMetaData")) {
          return md;
        }
        if (method.equals("getSchema")) {
          // What a pre-JDBC 4.1 driver raises for the unimplemented interface method.
          throw new AbstractMethodError("getSchema");
        }
        return UNHANDLED;
      });
      JdbcMetaData metadata = new JdbcMetaData(wrapped[0]);
      assertEquals(conn.getCatalog(), metadata.getCurrentCatalogName());
      assertEquals("", metadata.getCurrentSchemaName());
      assertEquals("", JdbcUtils.metadataSchema(md, null, false));
    }
  }

  @Test
  void schemaLessMysqlBareFilterSelectsOtherCatalogs() throws Exception {
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      try (Statement st = conn.createStatement()) {
        st.execute("CREATE TABLE T(A INT)");
        st.execute("ATTACH ':memory:' AS sales");
        st.execute("CREATE TABLE sales.main.ORDERS(ID INT)");
        st.execute("ATTACH ':memory:' AS stage");
        st.execute("CREATE TABLE stage.main.IMPORTS(ID INT)");
      }
      String current = conn.getCatalog();
      Connection[] wrapped = new Connection[1];
      DatabaseMetaData delegate = conn.getMetaData();
      DatabaseMetaData md = proxy(DatabaseMetaData.class, delegate, (method, args) -> {
        if (method.equals("getDatabaseProductName")) {
          return "MySQL";
        }
        if (method.equals("getConnection")) {
          return wrapped[0];
        }
        if (method.startsWith("supportsSchemas")) {
          return false;
        }
        if (method.equals("getSchemas")) {
          // Connector/J in catalog mode reports no schemas.
          return delegate.getSchemas(null, "no_such_schema");
        }
        if (method.equals("getTables") || method.equals("getColumns")) {
          ResultSet rows =
              method.equals("getTables") ? delegate.getTables((String) args[0], "main", "%", null)
                  : delegate.getColumns((String) args[0], "main", "%", "%");
          return proxy(ResultSet.class, rows, (name, parameters) -> {
            if (name.equals("getString") && "TABLE_SCHEM".equals(parameters[0])) {
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
        if (method.equals("createStatement")) {
          Statement st = conn.createStatement();
          return proxy(Statement.class, st, (name, parameters) -> {
            if (name.equals("executeQuery")
                && DatabaseSpecific.MYSQL.getCurrentSchemaQuery().equals(parameters[0])) {
              return st.executeQuery("SELECT current_database(), current_database()");
            }
            return UNHANDLED;
          });
        }
        return UNHANDLED;
      });
      for (String filter : List.of("sales", "SALES", "sal%")) {
        JdbcMetaData metadata = new JdbcMetaData(wrapped[0], List.of(filter));
        assertEquals(current, metadata.getCurrentCatalogName());
        assertNotNull(metadata.get("sales"), filter);
        assertNotNull(metadata.get("sales").get("").get("ORDERS"), filter);
        assertTrue(metadata.getCatalogsList().stream()
            .filter(catalog -> !catalog.tableCatalog.equalsIgnoreCase("sales"))
            .flatMap(catalog -> catalog.schemas.values().stream())
            .allMatch(schema -> schema.tables.isEmpty()), filter);
      }
      JdbcMetaData both = new JdbcMetaData(wrapped[0], List.of("sales", "stage"));
      assertNotNull(both.get("stage").get("").get("IMPORTS"));
      assertNotNull(both.get("sales").get("").get("ORDERS"));
    }
  }

  private Connection failingContext(Connection conn, SQLException catalogFailure,
      SQLException schemaFailure) throws SQLException {
    Connection[] profile = new Connection[1];
    DatabaseMetaData metadata = proxy(DatabaseMetaData.class, conn.getMetaData(), (name, args) -> {
      if (name.equals("getDatabaseProductName")) {
        return "PostgreSQL";
      }
      return name.equals("getConnection") ? profile[0] : UNHANDLED;
    });
    profile[0] = proxy(Connection.class, conn, (name, args) -> {
      if (name.equals("getMetaData")) {
        return metadata;
      }
      if (name.equals("getCatalog") && catalogFailure != null) {
        throw catalogFailure;
      }
      if (name.equals("getSchema") && schemaFailure != null) {
        throw schemaFailure;
      }
      if (name.equals("createStatement")) {
        return proxy(Statement.class, conn.createStatement(), (method, parameters) -> {
          if (method.equals("executeQuery")) {
            throw new SQLException("Current-context SQL failed");
          }
          return UNHANDLED;
        });
      }
      if (name.equals("commit") || name.equals("setAutoCommit")
          || name.equals("rollback") && args.length == 0) {
        throw new AssertionError("Scanner changed caller transaction");
      }
      return UNHANDLED;
    });
    return profile[0];
  }

  private Connection profileConnection(Connection conn, DatabaseSpecific type, boolean savepoints,
      List<String> calls) throws SQLException {
    Connection[] profile = new Connection[1];
    boolean[] aborted = {false};
    DatabaseMetaData delegate = conn.getMetaData();
    DatabaseMetaData md = proxy(DatabaseMetaData.class, delegate, (method, args) -> {
      if (method.equals("getDatabaseProductName")) {
        return type.identString;
      }
      if (method.equals("getConnection")) {
        return profile[0];
      }
      if (method.equals("supportsSavepoints")) {
        return savepoints;
      }
      if (method.equals("getCatalogs")) {
        assertFalse(aborted[0], "Fallback ran in an aborted transaction");
        return proxy(ResultSet.class, delegate.getCatalogs(), (name, parameters) -> {
          if (name.equals("next")) {
            return false; // Simulate a driver omitting the current catalog from discovery.
          }
          return UNHANDLED;
        });
      }
      if (method.equals("getSchemas") || method.equals("getTables")
          || method.equals("getColumns")) {
        assertFalse(aborted[0], "Fallback ran in an aborted transaction");
        ResultSet rows;
        if (method.equals("getSchemas")) {
          rows = delegate.getSchemas();
        } else if (method.equals("getTables")) {
          rows = delegate.getTables((String) args[0], (String) args[1], (String) args[2],
              (String[]) args[3]);
        } else {
          rows = delegate.getColumns((String) args[0], (String) args[1], (String) args[2],
              (String) args[3]);
        }
        return proxy(ResultSet.class, rows, (name, parameters) -> {
          if (name.equals("getString")
              && ("TABLE_CAT".equals(parameters[0]) || "TABLE_CATALOG".equals(parameters[0])
                  || "TABLE_SCHEM".equals(parameters[0]) && !method.equals("getSchemas"))) {
            return null;
          }
          return UNHANDLED;
        });
      }
      return UNHANDLED;
    });
    profile[0] = proxy(Connection.class, conn, (method, args) -> {
      if (method.equals("getMetaData")) {
        return md;
      }
      if (method.equals("commit") || method.equals("setAutoCommit")) {
        throw new AssertionError("Scanner changed caller transaction");
      }
      if (method.equals("rollback")) {
        assertEquals(1, args.length, "Scanner must never roll back the entire transaction");
        aborted[0] = false;
        calls.add("rollback-savepoint");
      }
      if (method.equals("prepareStatement")) {
        calls.add("probe");
        aborted[0] = true;
        throw new SQLException("Forced INFORMATION_SCHEMA failure");
      }
      if (method.equals("createStatement")) {
        Statement statement = conn.createStatement();
        return proxy(Statement.class, statement, (name, parameters) -> {
          if (name.equals("executeQuery")) {
            if (type.getCurrentSchemaQuery().equals(parameters[0])) {
              return statement.executeQuery("SELECT current_catalog(), current_schema()");
            }
            calls.add("probe");
            aborted[0] = true;
            throw new SQLException("Forced INFORMATION_SCHEMA failure");
          }
          return UNHANDLED;
        });
      }
      return UNHANDLED;
    });
    return profile[0];
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
