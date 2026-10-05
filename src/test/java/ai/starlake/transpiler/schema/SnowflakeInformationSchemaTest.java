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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SnowflakeInformationSchemaTest {
  @ParameterizedTest
  @CsvSource({"B,false", "B,true", "B\"Q,false"})
  void explicitHelperReadsRequestedCatalogAndPreservesPrecision(String catalog, boolean empty)
      throws Exception {
    try (Connection base = DriverManager.getConnection("jdbc:h2:mem:")) {
      List<String> queries = new ArrayList<>();
      List<Object> parameters = new ArrayList<>();
      Connection conn = snowflake(base, catalog, empty, queries, parameters);
      List<JdbcColumn> columns =
          new ArrayList<>(JdbcTable.getColumnsFromSchemaInformation(conn, catalog, "PUBLIC", "T"));
      assertTrue(queries.get(0)
          .contains("FROM \"" + catalog.replace("\"", "\"\"") + "\".INFORMATION_SCHEMA.COLUMNS"));
      assertEquals(List.of(catalog, "PUBLIC", "T"), parameters);
      assertEquals("A", conn.getCatalog());
      if (empty) {
        assertTrue(columns.isEmpty());
        return;
      }
      JdbcColumn column = columns.get(0);
      assertEquals(catalog, column.tableCatalog);
      assertEquals(12, column.columnSize);
      assertEquals(3, column.decimalDigits);
      assertNull(column.characterOctetLength);
      assertEquals(java.sql.Types.NUMERIC, column.dataType);
      JdbcMetaData metadata = new JdbcMetaData(catalog, "PUBLIC");
      metadata.setDatabaseType("SNOWFLAKE");
      metadata.addTable(catalog, "PUBLIC", "T", columns);
      JdbcMetaData restored = JdbcJSONSerializer
          .fromJson(new StringReader(JdbcJSONSerializer.toJson(metadata).toString()));
      JdbcColumn read = restored.get(catalog).get("PUBLIC").get("T").columns.get("AMOUNT");
      assertEquals(12, read.columnSize);
      assertEquals(3, read.decimalDigits);
      column.columnSize = null;
      column.decimalDigits = null;
      JdbcColumn nullRead = JdbcJSONSerializer
          .fromJson(new StringReader(JdbcJSONSerializer.toJson(metadata).toString())).get(catalog)
          .get("PUBLIC").get("T").columns.get("AMOUNT");
      assertNull(nullRead.columnSize);
      assertNull(nullRead.decimalDigits);
    }
  }

  @Test
  void explicitHelperUsesCurrentCatalogWhenNoneRequested() throws Exception {
    try (Connection base = DriverManager.getConnection("jdbc:h2:mem:")) {
      List<String> queries = new ArrayList<>();
      List<Object> parameters = new ArrayList<>();
      JdbcTable.getColumnsFromSchemaInformation(snowflake(base, "A", false, queries, parameters),
          null, "PUBLIC", "T");
      assertTrue(queries.get(0).contains("FROM \"A\".INFORMATION_SCHEMA.COLUMNS"));
      assertEquals(List.of("PUBLIC", "T"), parameters);
    }
  }

  @Test
  void numericHelpersDistinguishSqlNullZeroAndMissingFields() throws Exception {
    try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:");
        ResultSet rs = conn.createStatement()
            .executeQuery("SELECT CAST(NULL AS INTEGER) AS N, 0 AS Z, 12 AS P")) {
      if (!rs.next()) {
        fail("Expected a row with null, zero, and precision values");
      }
      assertNull(JdbcUtils.getIntSafe(rs, "N"));
      assertNull(JdbcUtils.getShortSafe(rs, "N"));
      assertEquals(0, JdbcUtils.getIntSafe(rs, "Z"));
      assertEquals((short) 0, JdbcUtils.getShortSafe(rs, "Z"));
      assertEquals(12, JdbcUtils.getIntSafe(rs, "P"));
      assertNull(JdbcUtils.getIntSafe(rs, "MISSING"));
      assertNull(JdbcUtils.getShortSafe(rs, "MISSING"));
    }
  }

  @Test
  void primitiveJdbcAccessorsHandleUnknownPrecisionAndScale() throws Exception {
    JdbcColumn column = new JdbcColumn("AMOUNT");
    column.columnSize = null;
    column.decimalDigits = null;
    JdbcResultSetMetaData metadata = new JdbcResultSetMetaData();
    metadata.add(column, null);
    assertEquals(0, metadata.getPrecision(1));
    assertEquals(0, metadata.getScale(1));
    assertEquals(0, metadata.getColumnDisplaySize(1));
    assertNull(column.columnSize);
    assertNull(column.decimalDigits);
    column.columnSize = 12;
    column.decimalDigits = 3;
    assertEquals(12, metadata.getPrecision(1));
    assertEquals(3, metadata.getScale(1));
  }

  @ParameterizedTest
  @CsvSource({"Snowflake,0,10", "Snowflake,,10", "Snowflake,10,10", "Snowflake,12,12",
      "PostgreSQL,0,0", "PostgreSQL,,"})
  void snowflakeDateSizeMatchesQueryPrecision(String product, Integer reported, Integer expected)
      throws Exception {
    try (Connection base = DriverManager.getConnection("jdbc:h2:mem:")) {
      DatabaseMetaData md = (DatabaseMetaData) Proxy.newProxyInstance(getClass().getClassLoader(),
          new Class<?>[] {DatabaseMetaData.class}, (proxy, method, args) -> {
            if (method.getName().equals("getDatabaseProductName")) {
              return product;
            }
            if (method.getName().equals("getColumns")) {
              java.sql.Statement st = base.createStatement();
              st.closeOnCompletion();
              return st.executeQuery("SELECT 'A' AS TABLE_CAT, 'PUBLIC' AS TABLE_SCHEM, "
                  + "'T' AS TABLE_NAME, 'D' AS COLUMN_NAME, " + java.sql.Types.DATE
                  + " AS DATA_TYPE, 'DATE' AS TYPE_NAME, CAST("
                  + (reported == null ? "NULL" : reported) + " AS INTEGER) AS COLUMN_SIZE");
            }
            return invoke(base.getMetaData(), method, args);
          });
      JdbcColumn date = JdbcTable.getColumns(md, "A", "PUBLIC", "T").iterator().next();
      assertEquals(expected, date.columnSize);
      JdbcMetaData metadata = new JdbcMetaData("A", "PUBLIC");
      metadata.addTable("T", date);
      JdbcMetaData restored = JdbcJSONSerializer
          .fromJson(new StringReader(JdbcJSONSerializer.toJson(metadata).toString()));
      assertEquals(expected, restored.get("A").get("PUBLIC").get("T").columns.get("D").columnSize);
    }
  }

  @Test
  void explicitDateHelperUsesTheSamePrecision() throws Exception {
    try (Connection base = DriverManager.getConnection("jdbc:h2:mem:")) {
      Connection profile =
          snowflake(base, "A", false, new ArrayList<>(), new ArrayList<>(), "DATE");
      JdbcColumn date =
          JdbcTable.getColumnsFromSchemaInformation(profile, "A", "PUBLIC", "T").iterator().next();
      assertEquals(10, date.columnSize);
    }
  }

  private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException ex) {
      throw ex.getCause();
    }
  }

  private Connection snowflake(Connection base, String catalog, boolean empty, List<String> queries,
      List<Object> parameters) throws SQLException {
    return snowflake(base, catalog, empty, queries, parameters, "NUMBER");
  }

  private Connection snowflake(Connection base, String catalog, boolean empty, List<String> queries,
      List<Object> parameters, String dataType) throws SQLException {
    Connection[] profile = new Connection[1];
    DatabaseMetaData md = (DatabaseMetaData) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {DatabaseMetaData.class}, (p, m, a) -> {
          if (m.getName().equals("getDatabaseProductName")) {
            return "Snowflake";
          }
          return m.getName().equals("getConnection") ? profile[0]
              : invoke(base.getMetaData(), m, a);
        });
    profile[0] = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {Connection.class}, (p, m, a) -> {
          if (m.getName().equals("getMetaData")) {
            return md;
          }
          if (m.getName().equals("getCatalog")) {
            return "A";
          }
          if (m.getName().equals("prepareStatement")) {
            queries.add((String) a[0]);
            PreparedStatement st = base.prepareStatement("SELECT '" + catalog.replace("'", "''")
                + "' AS TABLE_CATALOG, 'PUBLIC' AS TABLE_SCHEMA, "
                + "'T' AS TABLE_NAME, 'AMOUNT' AS COLUMN_NAME, 1 AS ORDINAL_POSITION, "
                + "CAST(NULL AS VARCHAR) AS COLUMN_DEFAULT, 'YES' AS IS_NULLABLE, " + "'" + dataType
                + "' AS DATA_TYPE, CAST(NULL AS INTEGER) AS CHARACTER_MAXIMUM_LENGTH, "
                + ("DATE".equals(dataType) ? "NULL" : "12")
                + " AS NUMERIC_PRECISION, 3 AS NUMERIC_SCALE, "
                + "CAST(NULL AS VARCHAR) AS COMMENT, 'NO' AS IS_IDENTITY"
                + (empty ? " WHERE FALSE" : ""));
            return Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {PreparedStatement.class}, (sp, sm, sa) -> {
                  if (sm.getName().equals("setObject")) {
                    parameters.add(sa[1]);
                    return null;
                  }
                  return invoke(st, sm, sa);
                });
          }
          return invoke(base, m, a);
        });
    return profile[0];
  }
}
