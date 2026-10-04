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
