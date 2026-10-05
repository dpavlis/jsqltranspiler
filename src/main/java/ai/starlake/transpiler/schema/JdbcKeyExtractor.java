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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

/**
 * Schema-scoped, opt-in catalog enrichment; speculative queries use the shared transaction guard.
 */
final class JdbcKeyExtractor {
  private static final Logger LOGGER = Logger.getLogger(JdbcKeyExtractor.class.getName());

  private JdbcKeyExtractor() {}

  static void enrich(Connection conn, JdbcMetaData metadata, JdbcMetaDataOptions options)
      throws SQLException {
    if (!options.primaryKeys && !options.foreignKeys && !options.comments && !options.columnDetails
        && !options.indices) {
      return;
    }
    DatabaseMetaData md = conn.getMetaData();
    JdbcUtils.DatabaseSpecific type =
        JdbcUtils.DatabaseSpecific.getType(md.getDatabaseProductName());
    for (JdbcCatalog catalog : metadata.getCatalogsList()) {
      for (JdbcSchema schema : catalog.schemas.values()) {
        if (schema.tables.isEmpty() || !type.processSchema(schema.tableSchema)) {
          continue;
        }
        if (options.primaryKeys && !bulkPrimaryKeys(conn, type, schema)) {
          for (JdbcTable table : schema.tables.values()) {
            optionalJdbc(conn, () -> {
              table.getPrimaryKey(md);
              return null;
            });
          }
        }
        if (options.foreignKeys && !bulkForeignKeys(conn, type, schema)) {
          for (JdbcTable table : schema.tables.values()) {
            optionalJdbc(conn, () -> {
              try (ResultSet rs =
                  md.getImportedKeys(table.tableCatalog, table.tableSchema, table.tableName)) {
                readForeignKeys(rs, schema);
              }
              return null;
            });
          }
        }
        if (options.comments || options.columnDetails) {
          enrichDetails(conn, type, schema, options);
        }
        if (options.indices) {
          for (JdbcTable table : schema.tables.values()) {
            optionalJdbc(conn, () -> {
              table.getIndices(md, true);
              return null;
            });
          }
        }
      }
    }
  }

  private static <T> boolean optionalJdbc(Connection conn, JdbcUtils.MetadataSupplier<T> supplier)
      throws SQLException {
    try {
      if (conn.getAutoCommit() || !conn.getMetaData().supportsSavepoints()) {
        supplier.get();
      } else {
        JdbcUtils.metadataProbe(conn, supplier);
      }
      return true;
    } catch (SQLFeatureNotSupportedException unsupported) {
      LOGGER.fine("Optional JDBC metadata is unsupported: " + unsupported.getMessage());
      return false;
    }
  }

  private static boolean probe(Connection conn, JdbcUtils.MetadataSupplier<Void> supplier)
      throws SQLException {
    try {
      JdbcUtils.metadataProbe(conn, supplier);
      return true;
    } catch (SQLException ex) {
      if (ex instanceof JdbcUtils.MetadataRecoveryException) {
        throw ex;
      }
      LOGGER.fine("Bulk metadata unavailable; use JDBC: " + ex.getMessage());
      return false;
    }
  }

  private static String prefix(Connection conn, JdbcUtils.DatabaseSpecific type, JdbcSchema schema)
      throws SQLException {
    if (type == JdbcUtils.DatabaseSpecific.POSTGRESQL || type == JdbcUtils.DatabaseSpecific.MYSQL
        || schema.tableCatalog == null || schema.tableCatalog.isEmpty()) {
      return "";
    }
    String quote = conn.getMetaData().getIdentifierQuoteString().trim();
    if (quote.isEmpty()) {
      quote = "\"";
    }
    String close = "[".equals(quote) ? "]" : quote;
    return quote + schema.tableCatalog.replace(close, close + close) + close + ".";
  }

  private static String schemaName(JdbcUtils.DatabaseSpecific type, JdbcSchema schema) {
    return type == JdbcUtils.DatabaseSpecific.MYSQL && schema.tableSchema.isEmpty()
        ? schema.tableCatalog
        : schema.tableSchema;
  }

  private static boolean catalogFilter(JdbcUtils.DatabaseSpecific type, JdbcSchema schema) {
    return type.getKeyStrategy() == JdbcUtils.KeyStrategy.INFORMATION_SCHEMA
        && type != JdbcUtils.DatabaseSpecific.MYSQL && schema.tableCatalog != null
        && !schema.tableCatalog.isEmpty();
  }

  private static boolean bulkPrimaryKeys(Connection conn, JdbcUtils.DatabaseSpecific type,
      JdbcSchema schema) throws SQLException {
    if (type.getKeyStrategy() == JdbcUtils.KeyStrategy.JDBC) {
      return false;
    }
    return probe(conn, () -> {
      String query;
      if (type.getKeyStrategy() == JdbcUtils.KeyStrategy.ORACLE) {
        query = "SELECT c.TABLE_NAME, c.CONSTRAINT_NAME AS PK_NAME, k.COLUMN_NAME, "
            + "k.POSITION AS KEY_SEQ FROM ALL_CONSTRAINTS c JOIN ALL_CONS_COLUMNS k "
            + "ON c.OWNER=k.OWNER AND c.CONSTRAINT_NAME=k.CONSTRAINT_NAME "
            + "AND c.TABLE_NAME=k.TABLE_NAME WHERE c.CONSTRAINT_TYPE='P' AND c.OWNER=? "
            + "ORDER BY c.TABLE_NAME, k.POSITION";
      } else {
        String info = prefix(conn, type, schema) + "information_schema.";
        query = "SELECT c.TABLE_NAME, c.CONSTRAINT_NAME AS PK_NAME, k.COLUMN_NAME, "
            + "k.ORDINAL_POSITION AS KEY_SEQ FROM " + info + "table_constraints c JOIN " + info
            + "key_column_usage k ON c.CONSTRAINT_CATALOG=k.CONSTRAINT_CATALOG "
            + "AND c.CONSTRAINT_SCHEMA=k.CONSTRAINT_SCHEMA AND c.CONSTRAINT_NAME=k.CONSTRAINT_NAME "
            + "AND c.TABLE_NAME=k.TABLE_NAME WHERE c.CONSTRAINT_TYPE='PRIMARY KEY' "
            + "AND c.TABLE_SCHEMA=?" + (catalogFilter(type, schema) ? " AND c.TABLE_CATALOG=?" : "")
            + " ORDER BY c.TABLE_NAME, k.ORDINAL_POSITION";
      }
      try (PreparedStatement st = conn.prepareStatement(query)) {
        st.setString(1, schemaName(type, schema));
        if (catalogFilter(type, schema)) {
          st.setString(2, schema.tableCatalog);
        }
        try (ResultSet rs = st.executeQuery()) {
          Map<JdbcTable, JdbcPrimaryKey> keys = new LinkedHashMap<>();
          Map<JdbcTable, TreeMap<Integer, String>> columns = new LinkedHashMap<>();
          while (rs.next()) {
            JdbcTable table = schema.get(rs.getString("TABLE_NAME"));
            if (table != null) {
              keys.computeIfAbsent(table, t -> new JdbcPrimaryKey(t.tableCatalog, t.tableSchema,
                  t.tableName, JdbcUtils.getStringSafe(rs, "PK_NAME")));
              columns.computeIfAbsent(table, t -> new TreeMap<>()).put(rs.getInt("KEY_SEQ"),
                  rs.getString("COLUMN_NAME"));
            }
          }
          keys.forEach((table, key) -> {
            key.columnNames.addAll(columns.get(table).values());
            table.primaryKey = key;
          });
        }
      }
      return null;
    });
  }

  private static boolean bulkForeignKeys(Connection conn, JdbcUtils.DatabaseSpecific type,
      JdbcSchema schema) throws SQLException {
    return probe(conn, () -> {
      // pgJDBC accepts null table and returns all imported keys of the schema.
      if (type == JdbcUtils.DatabaseSpecific.POSTGRESQL
          || type.getKeyStrategy() == JdbcUtils.KeyStrategy.JDBC) {
        try (ResultSet rs =
            conn.getMetaData().getImportedKeys(schema.tableCatalog, schema.tableSchema, null)) {
          readForeignKeys(rs, schema);
        }
      } else {
        String query = foreignKeyQuery(conn, type, schema);
        try (PreparedStatement st = conn.prepareStatement(query)) {
          st.setString(1, schemaName(type, schema));
          if (catalogFilter(type, schema) && type != JdbcUtils.DatabaseSpecific.MSSQL) {
            st.setString(2, schema.tableCatalog);
          }
          try (ResultSet rs = st.executeQuery()) {
            readForeignKeys(rs, schema);
          }
        }
      }
      return null;
    });
  }

  private static String foreignKeyQuery(Connection conn, JdbcUtils.DatabaseSpecific type,
      JdbcSchema schema) throws SQLException {
    if (type == JdbcUtils.DatabaseSpecific.ORACLE) {
      return "SELECT f.TABLE_NAME AS FKTABLE_NAME, f.OWNER AS FKTABLE_SCHEM, "
          + "f.CONSTRAINT_NAME AS FK_NAME, fc.COLUMN_NAME AS FKCOLUMN_NAME, "
          + "p.TABLE_NAME AS PKTABLE_NAME, p.OWNER AS PKTABLE_SCHEM, "
          + "p.CONSTRAINT_NAME AS PK_NAME, pc.COLUMN_NAME AS PKCOLUMN_NAME, "
          + "fc.POSITION AS KEY_SEQ, 'NO ACTION' AS UPDATE_RULE, f.DELETE_RULE, "
          + "CASE WHEN f.DEFERRABLE='NOT DEFERRABLE' THEN 7 "
          + "WHEN f.DEFERRED='DEFERRED' THEN 5 ELSE 6 END AS DEFERRABILITY "
          + "FROM ALL_CONSTRAINTS f JOIN ALL_CONS_COLUMNS fc ON f.OWNER=fc.OWNER "
          + "AND f.CONSTRAINT_NAME=fc.CONSTRAINT_NAME JOIN ALL_CONSTRAINTS p "
          + "ON f.R_OWNER=p.OWNER AND f.R_CONSTRAINT_NAME=p.CONSTRAINT_NAME "
          + "JOIN ALL_CONS_COLUMNS pc ON p.OWNER=pc.OWNER AND p.CONSTRAINT_NAME=pc.CONSTRAINT_NAME "
          + "AND fc.POSITION=pc.POSITION WHERE f.CONSTRAINT_TYPE='R' AND f.OWNER=? "
          + "ORDER BY f.TABLE_NAME, f.CONSTRAINT_NAME, fc.POSITION";
    }
    String prefix = prefix(conn, type, schema);
    if (type == JdbcUtils.DatabaseSpecific.MSSQL) {
      // SQL Server has no position_in_unique_constraint; sys views give the actual paired columns.
      String sys = prefix + "sys.";
      return "SELECT ft.name AS FKTABLE_NAME, fs.name AS FKTABLE_SCHEM, "
          + "fk.name AS FK_NAME, fc.name AS FKCOLUMN_NAME, pt.name AS PKTABLE_NAME, "
          + "ps.name AS PKTABLE_SCHEM, pc.name AS PKCOLUMN_NAME, pi.name AS PK_NAME, "
          + "k.constraint_column_id AS KEY_SEQ, fk.update_referential_action_desc AS UPDATE_RULE, "
          + "fk.delete_referential_action_desc AS DELETE_RULE, 7 AS DEFERRABILITY FROM " + sys
          + "foreign_keys fk JOIN " + sys
          + "foreign_key_columns k ON fk.object_id=k.constraint_object_id JOIN " + sys
          + "tables ft ON ft.object_id=k.parent_object_id JOIN " + sys
          + "schemas fs ON fs.schema_id=ft.schema_id JOIN " + sys
          + "columns fc ON fc.object_id=ft.object_id AND fc.column_id=k.parent_column_id JOIN "
          + sys + "tables pt ON pt.object_id=k.referenced_object_id JOIN " + sys
          + "schemas ps ON ps.schema_id=pt.schema_id JOIN " + sys
          + "columns pc ON pc.object_id=pt.object_id AND pc.column_id=k.referenced_column_id "
          + "JOIN " + sys + "indexes pi ON pi.object_id=pt.object_id "
          + "AND pi.index_id=fk.key_index_id WHERE fs.name=? "
          + "ORDER BY ft.name, fk.name, k.constraint_column_id";
    }
    String info = prefix + "information_schema.";
    if (type == JdbcUtils.DatabaseSpecific.MYSQL) {
      String fkScope =
          schema.tableSchema.isEmpty() ? "f.TABLE_SCHEMA AS FKTABLE_CAT, NULL AS FKTABLE_SCHEM, "
              : "f.TABLE_SCHEMA AS FKTABLE_SCHEM, ";
      String pkScope = schema.tableSchema.isEmpty()
          ? "f.REFERENCED_TABLE_SCHEMA AS PKTABLE_CAT, NULL AS PKTABLE_SCHEM, "
          : "f.REFERENCED_TABLE_SCHEMA AS PKTABLE_SCHEM, ";
      return "SELECT f.TABLE_NAME AS FKTABLE_NAME, " + fkScope
          + "f.CONSTRAINT_NAME AS FK_NAME, f.COLUMN_NAME AS FKCOLUMN_NAME, "
          + "f.REFERENCED_TABLE_NAME AS PKTABLE_NAME, " + pkScope
          + "f.REFERENCED_COLUMN_NAME AS PKCOLUMN_NAME, r.UNIQUE_CONSTRAINT_NAME AS PK_NAME, "
          + "f.ORDINAL_POSITION AS KEY_SEQ, r.UPDATE_RULE, r.DELETE_RULE, 7 AS DEFERRABILITY "
          + "FROM " + info + "key_column_usage f JOIN " + info
          + "referential_constraints r ON f.CONSTRAINT_SCHEMA=r.CONSTRAINT_SCHEMA "
          + "AND f.CONSTRAINT_NAME=r.CONSTRAINT_NAME AND f.TABLE_NAME=r.TABLE_NAME "
          + "WHERE f.TABLE_SCHEMA=? AND f.REFERENCED_TABLE_NAME IS NOT NULL "
          + "ORDER BY f.TABLE_NAME, f.CONSTRAINT_NAME, f.ORDINAL_POSITION";
    }
    return "SELECT f.TABLE_NAME AS FKTABLE_NAME, f.TABLE_CATALOG AS FKTABLE_CAT, "
        + "f.TABLE_SCHEMA AS FKTABLE_SCHEM, f.CONSTRAINT_NAME AS FK_NAME, "
        + "f.COLUMN_NAME AS FKCOLUMN_NAME, p.TABLE_NAME AS PKTABLE_NAME, "
        + "p.TABLE_CATALOG AS PKTABLE_CAT, p.TABLE_SCHEMA AS PKTABLE_SCHEM, "
        + "p.COLUMN_NAME AS PKCOLUMN_NAME, p.CONSTRAINT_NAME AS PK_NAME, "
        + "f.ORDINAL_POSITION AS KEY_SEQ, r.UPDATE_RULE, r.DELETE_RULE, 7 AS DEFERRABILITY "
        + "FROM " + info + "referential_constraints r JOIN " + info + "key_column_usage f "
        + "ON r.CONSTRAINT_CATALOG=f.CONSTRAINT_CATALOG "
        + "AND r.CONSTRAINT_SCHEMA=f.CONSTRAINT_SCHEMA AND r.CONSTRAINT_NAME=f.CONSTRAINT_NAME "
        + "JOIN " + info + "key_column_usage p "
        + "ON r.UNIQUE_CONSTRAINT_CATALOG=p.CONSTRAINT_CATALOG "
        + "AND r.UNIQUE_CONSTRAINT_SCHEMA=p.CONSTRAINT_SCHEMA "
        + "AND r.UNIQUE_CONSTRAINT_NAME=p.CONSTRAINT_NAME "
        + "AND f.POSITION_IN_UNIQUE_CONSTRAINT=p.ORDINAL_POSITION WHERE f.TABLE_SCHEMA=?"
        + (catalogFilter(type, schema) ? " AND f.TABLE_CATALOG=?" : "")
        + " ORDER BY f.TABLE_NAME, f.CONSTRAINT_NAME, f.ORDINAL_POSITION";
  }

  private static void readForeignKeys(ResultSet rs, JdbcSchema schema) throws SQLException {
    Map<JdbcTable, List<JdbcReference>> references = new LinkedHashMap<>();
    Map<JdbcReference, TreeMap<Integer, String[]>> columns = new IdentityHashMap<>();
    JdbcReference current = null;
    while (rs.next()) {
      JdbcTable table = schema.get(rs.getString("FKTABLE_NAME"));
      String fkSchema = JdbcUtils.getStringSafe(rs, "FKTABLE_SCHEM");
      String fkCatalog = JdbcUtils.getStringSafe(rs, "FKTABLE_CAT");
      if (table == null
          || fkSchema != null && !fkSchema.isEmpty() && !schema.tableSchema.isEmpty()
              && !fkSchema.equalsIgnoreCase(schema.tableSchema)
          || fkCatalog != null && !fkCatalog.isEmpty() && !schema.tableCatalog.isEmpty()
              && !fkCatalog.equalsIgnoreCase(schema.tableCatalog)) {
        continue;
      }
      String fkName = JdbcUtils.getStringSafe(rs, "FK_NAME");
      String pkCatalog = JdbcUtils.getStringSafe(rs, "PKTABLE_CAT", schema.tableCatalog);
      String pkSchema = JdbcUtils.getStringSafe(rs, "PKTABLE_SCHEM", "");
      String pkTable = rs.getString("PKTABLE_NAME");
      int sequence = rs.getInt("KEY_SEQ");
      List<JdbcReference> keys = references.computeIfAbsent(table, t -> new ArrayList<>());
      JdbcReference key = null;
      for (JdbcReference candidate : keys) {
        if (fkName != null && fkName.equals(candidate.fkName)
            && pkTable.equals(candidate.pkTableName)
            && java.util.Objects.equals(pkSchema, candidate.pkTableSchema)
            && java.util.Objects.equals(pkCatalog, candidate.pkTableCatalog)) {
          key = candidate;
          break;
        }
      }
      if (fkName == null && sequence > 1 && current != null
          && table.tableName.equals(current.fkTableName) && pkTable.equals(current.pkTableName)) {
        key = current;
      }
      if (key == null) {
        key = new JdbcReference(pkCatalog, pkSchema, pkTable, table.tableCatalog, table.tableSchema,
            table.tableName, readRule(rs, "UPDATE_RULE"), readRule(rs, "DELETE_RULE"), fkName,
            JdbcUtils.getStringSafe(rs, "PK_NAME"), JdbcUtils.getShortSafe(rs, "DEFERRABILITY"));
        keys.add(key);
        columns.put(key, new TreeMap<>());
      }
      columns.get(key).put(sequence,
          new String[] {rs.getString("FKCOLUMN_NAME"), rs.getString("PKCOLUMN_NAME")});
      current = key;
    }
    references.forEach((table, keys) -> {
      keys.forEach(key -> key.columns.addAll(columns.get(key).values()));
      table.foreignKeys.addAll(keys);
    });
  }

  private static Short readRule(ResultSet rs, String column) {
    String value = JdbcUtils.getStringSafe(rs, column);
    if (value == null) {
      return null;
    }
    try {
      return Short.valueOf(value);
    } catch (NumberFormatException namedRule) {
      return rule(value);
    }
  }

  static Short rule(String value) {
    if (value == null) {
      return null;
    }
    switch (value.replace(' ', '_').toUpperCase(java.util.Locale.ROOT)) {
      case "CASCADE":
        return DatabaseMetaData.importedKeyCascade;
      case "RESTRICT":
        return DatabaseMetaData.importedKeyRestrict;
      case "SET_NULL":
        return DatabaseMetaData.importedKeySetNull;
      case "NO_ACTION":
        return DatabaseMetaData.importedKeyNoAction;
      case "SET_DEFAULT":
        return DatabaseMetaData.importedKeySetDefault;
      default:
        return null;
    }
  }

  static String ruleName(Short value) {
    if (value == null) {
      return null;
    }
    switch (value) {
      case DatabaseMetaData.importedKeyCascade:
        return "CASCADE";
      case DatabaseMetaData.importedKeyRestrict:
        return "RESTRICT";
      case DatabaseMetaData.importedKeySetNull:
        return "SET_NULL";
      case DatabaseMetaData.importedKeyNoAction:
        return "NO_ACTION";
      case DatabaseMetaData.importedKeySetDefault:
        return "SET_DEFAULT";
      default:
        return null;
    }
  }

  private static void enrichDetails(Connection conn, JdbcUtils.DatabaseSpecific type,
      JdbcSchema schema, JdbcMetaDataOptions options) throws SQLException {
    if (options.columnDetails && type != JdbcUtils.DatabaseSpecific.H2
        && !type.usesJdbcMetadata()) {
      // The generic INFORMATION_SCHEMA mapping cannot infer vendor identity/generated flags.
      // Supplement it with a schema-wide native read, never one call per table.
      optionalJdbc(conn, () -> {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getColumns(schema.tableCatalog,
            JdbcTable.escapeSchema(md, schema.tableSchema), "%", "%")) {
          while (rs.next()) {
            String sourceCatalog = JdbcUtils.getStringSafe(rs, "TABLE_CAT", schema.tableCatalog);
            String sourceSchema = JdbcUtils.getStringSafe(rs, "TABLE_SCHEM", schema.tableSchema);
            if (!sourceCatalog.equalsIgnoreCase(schema.tableCatalog)
                || !sourceSchema.equalsIgnoreCase(schema.tableSchema)) {
              continue;
            }
            JdbcTable table = schema.get(rs.getString("TABLE_NAME"));
            JdbcColumn column =
                table == null ? null : table.columns.get(rs.getString("COLUMN_NAME"));
            if (column != null) {
              // Oracle exposes COLUMN_DEF as LONG: consume it before accessing later fields.
              if (JdbcUtils.findColumnSafe(rs, "COLUMN_DEF") >= 0) {
                column.columnDefinition = rs.getString("COLUMN_DEF");
              }
              Integer position = JdbcUtils.getIntSafe(rs, "ORDINAL_POSITION");
              if (position != null) {
                column.ordinalPosition = position;
              }
              column.isAutomaticIncrement = JdbcUtils.getStringSafe(rs, "IS_AUTOINCREMENT", "");
              column.isGeneratedColumn = JdbcUtils.getStringSafe(rs, "IS_GENERATEDCOLUMN", "");
            }
          }
        }
        return null;
      });
    }
    if (type == JdbcUtils.DatabaseSpecific.H2) {
      probe(conn, () -> {
        if (options.comments) {
          try (PreparedStatement st = conn.prepareStatement("SELECT TABLE_NAME, REMARKS FROM "
              + prefix(conn, type, schema) + "information_schema.tables WHERE TABLE_SCHEMA=?")) {
            st.setString(1, schema.tableSchema);
            try (ResultSet rs = st.executeQuery()) {
              while (rs.next()) {
                JdbcTable table = schema.get(rs.getString("TABLE_NAME"));
                if (table != null) {
                  table.remarks = rs.getString("REMARKS");
                }
              }
            }
          }
        }
        try (PreparedStatement st = conn.prepareStatement("SELECT * FROM "
            + prefix(conn, type, schema) + "information_schema.columns WHERE TABLE_SCHEMA=?")) {
          st.setString(1, schema.tableSchema);
          try (ResultSet rs = st.executeQuery()) {
            while (rs.next()) {
              JdbcTable table = schema.get(rs.getString("TABLE_NAME"));
              JdbcColumn column =
                  table == null ? null : table.columns.get(rs.getString("COLUMN_NAME"));
              if (column != null) {
                if (options.comments) {
                  column.remarks = rs.getString("REMARKS");
                }
                if (options.columnDetails) {
                  column.isGeneratedColumn =
                      "ALWAYS".equals(rs.getString("IS_GENERATED")) ? "YES" : "NO";
                }
              }
            }
          }
        }
        return null;
      });
    } else if (options.comments && type == JdbcUtils.DatabaseSpecific.POSTGRESQL) {
      probe(conn, () -> {
        String query = "SELECT c.relname AS TABLE_NAME, a.attname AS COLUMN_NAME, "
            + "pg_catalog.obj_description(c.oid, 'pg_class') AS TABLE_COMMENT, "
            + "pg_catalog.col_description(c.oid, a.attnum) AS COLUMN_COMMENT "
            + "FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
            + "LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid "
            + "AND a.attnum>0 AND NOT a.attisdropped WHERE n.nspname=?";
        try (PreparedStatement st = conn.prepareStatement(query)) {
          st.setString(1, schema.tableSchema);
          try (ResultSet rs = st.executeQuery()) {
            while (rs.next()) {
              JdbcTable table = schema.get(rs.getString("TABLE_NAME"));
              if (table != null) {
                table.remarks = rs.getString("TABLE_COMMENT");
                JdbcColumn column = table.columns.get(rs.getString("COLUMN_NAME"));
                if (column != null) {
                  column.remarks = rs.getString("COLUMN_COMMENT");
                }
              }
            }
          }
        }
        return null;
      });
    }
    if (options.comments && (type == JdbcUtils.DatabaseSpecific.MSSQL
        || type == JdbcUtils.DatabaseSpecific.MYSQL || type == JdbcUtils.DatabaseSpecific.SNOWFLAKE
        || type == JdbcUtils.DatabaseSpecific.ORACLE
        || type == JdbcUtils.DatabaseSpecific.DUCKDB)) {
      probe(conn, () -> {
        String query = commentsQuery(conn, type, schema);
        try (PreparedStatement st = conn.prepareStatement(query)) {
          st.setString(1, schemaName(type, schema));
          if (type == JdbcUtils.DatabaseSpecific.DUCKDB && catalogFilter(type, schema)) {
            st.setString(2, schema.tableCatalog);
            st.setString(3, schema.tableSchema);
            st.setString(4, schema.tableCatalog);
          } else if (type != JdbcUtils.DatabaseSpecific.MSSQL) {
            st.setString(2, schemaName(type, schema));
          }
          try (ResultSet rs = st.executeQuery()) {
            while (rs.next()) {
              JdbcTable table = schema.get(rs.getString("TABLE_NAME"));
              if (table != null) {
                String columnName = rs.getString("COLUMN_NAME");
                if (columnName == null) {
                  table.remarks = rs.getString("COMMENT_TEXT");
                } else {
                  JdbcColumn column = table.columns.get(columnName);
                  if (column != null) {
                    column.remarks = rs.getString("COMMENT_TEXT");
                  }
                }
              }
            }
          }
        }
        return null;
      });
    }
    // Other profiles keep the remarks/details already returned by JDBC or INFORMATION_SCHEMA.
  }

  private static String commentsQuery(Connection conn, JdbcUtils.DatabaseSpecific type,
      JdbcSchema schema) throws SQLException {
    if (type == JdbcUtils.DatabaseSpecific.MSSQL) {
      String sys = prefix(conn, type, schema) + "sys.";
      return "SELECT t.name AS TABLE_NAME, c.name AS COLUMN_NAME, "
          + "CONVERT(nvarchar(max), e.value) AS COMMENT_TEXT FROM " + sys + "objects t JOIN " + sys
          + "schemas s ON s.schema_id=t.schema_id LEFT JOIN " + sys
          + "extended_properties e ON e.class=1 AND e.major_id=t.object_id "
          + "AND e.name='MS_Description' LEFT JOIN " + sys
          + "columns c ON c.object_id=t.object_id AND c.column_id=e.minor_id "
          + "WHERE s.name=? AND t.type IN ('U','V')";
    }
    if (type == JdbcUtils.DatabaseSpecific.ORACLE) {
      return "SELECT TABLE_NAME, NULL AS COLUMN_NAME, COMMENTS AS COMMENT_TEXT "
          + "FROM ALL_TAB_COMMENTS WHERE OWNER=? UNION ALL "
          + "SELECT TABLE_NAME, COLUMN_NAME, COMMENTS AS COMMENT_TEXT "
          + "FROM ALL_COL_COMMENTS WHERE OWNER=?";
    }
    if (type == JdbcUtils.DatabaseSpecific.DUCKDB) {
      String catalogClause = catalogFilter(type, schema) ? " AND database_name=?" : "";
      return "SELECT table_name AS TABLE_NAME, NULL AS COLUMN_NAME, comment AS COMMENT_TEXT "
          + "FROM duckdb_tables() WHERE schema_name=?" + catalogClause + " UNION ALL "
          + "SELECT table_name AS TABLE_NAME, column_name AS COLUMN_NAME, comment AS COMMENT_TEXT "
          + "FROM duckdb_columns() WHERE schema_name=?" + catalogClause;
    }
    String info = prefix(conn, type, schema) + "information_schema.";
    String tableComment = type == JdbcUtils.DatabaseSpecific.MYSQL ? "TABLE_COMMENT" : "COMMENT";
    String columnComment = type == JdbcUtils.DatabaseSpecific.MYSQL ? "COLUMN_COMMENT" : "COMMENT";
    return "SELECT TABLE_NAME, NULL AS COLUMN_NAME, " + tableComment + " AS COMMENT_TEXT FROM "
        + info + "tables WHERE TABLE_SCHEMA=? UNION ALL SELECT TABLE_NAME, COLUMN_NAME, "
        + columnComment + " AS COMMENT_TEXT FROM " + info + "columns WHERE TABLE_SCHEMA=?";
  }

  static void copyKeys(JdbcTable source, JdbcTable target) {
    if (source.primaryKey != null) {
      JdbcPrimaryKey key = source.primaryKey;
      target.primaryKey =
          new JdbcPrimaryKey(key.tableCatalog, key.tableSchema, key.tableName, key.primaryKeyName);
      target.primaryKey.columnNames.addAll(key.columnNames);
    }
    for (JdbcReference key : source.foreignKeys) {
      JdbcReference copy = new JdbcReference(key.pkTableCatalog, key.pkTableSchema, key.pkTableName,
          key.fkTableCatalog, key.fkTableSchema, key.fkTableName, key.updateRule, key.deleteRule,
          key.fkName, key.pkName, key.deferrability);
      key.columns.forEach(pair -> copy.columns.add(pair.clone()));
      target.foreignKeys.add(copy);
    }
    for (JdbcIndex index : source.indices.values()) {
      JdbcIndex copy = new JdbcIndex(index.tableCatalog, index.tableSchema, index.tableName,
          index.nonUnique, index.indexQualifier, index.indexName, index.type);
      index.columns.forEach((position, column) -> copy.put(position, column.columnName,
          column.ascOrDesc, column.cardinality, column.pages, column.filterCondition));
      target.indices.put(copy.indexName, copy);
    }
  }
}
