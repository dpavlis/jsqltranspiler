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

import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Savepoint;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLException;
import java.util.Locale;

public class JdbcUtils {

  @FunctionalInterface
  interface MetadataSupplier<T> {
    T get() throws SQLException;
  }

  /** A failed savepoint operation must propagate instead of attempting more queries. */
  static final class MetadataRecoveryException extends SQLException {
    private static final long serialVersionUID = 1L;

    MetadataRecoveryException(SQLException cause) {
      super("Cannot safely recover metadata probe transaction", cause);
    }
  }

  /** Runs a speculative query without committing or rolling back the caller's earlier work. */
  static <T> T metadataProbe(Connection conn, MetadataSupplier<T> query) throws SQLException {
    if (conn.getAutoCommit()) {
      return query.get();
    }
    if (!conn.getMetaData().supportsSavepoints()) {
      throw new SQLFeatureNotSupportedException(
          "Skip metadata SQL probe without savepoint support");
    }
    Savepoint savepoint;
    try {
      savepoint = conn.setSavepoint();
    } catch (SQLException ex) {
      throw new MetadataRecoveryException(ex);
    }
    T result;
    try {
      result = query.get();
    } catch (SQLException | RuntimeException ex) {
      // Undo the probe on any failure, so an unexpected runtime error cannot leave the
      // savepoint and partial probe work inside the caller's transaction.
      try {
        conn.rollback(savepoint);
        releaseProbeSavepoint(conn, savepoint);
      } catch (SQLException recovery) {
        recovery.addSuppressed(ex);
        throw new MetadataRecoveryException(recovery);
      }
      throw ex;
    }
    try {
      releaseProbeSavepoint(conn, savepoint);
    } catch (SQLException ex) {
      throw new MetadataRecoveryException(ex);
    }
    return result;
  }

  private static void releaseProbeSavepoint(Connection conn, Savepoint savepoint)
      throws SQLException {
    DatabaseSpecific type = DatabaseSpecific.getType(conn.getMetaData().getDatabaseProductName());
    // Oracle and SQL Server support rollback to savepoints but cannot release them.
    // They are discarded when the caller ends the transaction.
    if (type != DatabaseSpecific.ORACLE && type != DatabaseSpecific.MSSQL) {
      try {
        conn.releaseSavepoint(savepoint);
      } catch (SQLFeatureNotSupportedException unsupported) {
        // JDBC distinguishes unsupported release from a failed transaction operation.
      }
    }
  }

  static String quoteIdentifier(DatabaseMetaData metadata, String identifier) throws SQLException {
    String quote = metadata.getIdentifierQuoteString();
    quote = quote == null ? "" : quote.trim();
    if (quote.isEmpty()) {
      quote = "\"";
    }
    String close = "[".equals(quote) ? "]" : quote;
    return quote + identifier.replace(close, close + close) + close;
  }

  static String metadataCatalog(DatabaseMetaData metaData, String requestedCatalog)
      throws SQLException {
    if (requestedCatalog != null && !requestedCatalog.isEmpty()) {
      return requestedCatalog;
    }
    try {
      // Respect catalog-less drivers even if they expose a connection database name.
      if (DatabaseSpecific.getType(metaData.getDatabaseProductName()) != DatabaseSpecific.POSTGRESQL
          && !metaData.supportsCatalogsInTableDefinitions()
          && !metaData.supportsCatalogsInDataManipulation()) {
        return "";
      }
      String catalog = metaData.getConnection().getCatalog();
      return catalog == null ? "" : catalog;
    } catch (SQLFeatureNotSupportedException ex) {
      return "";
    }
  }

  /** Connector/J's schema mode uses the synthetic catalog "def", not a SQL qualifier. */
  static String metadataCatalogValue(DatabaseMetaData metadata, String catalog)
      throws SQLException {
    if ("def".equalsIgnoreCase(catalog)
        && DatabaseSpecific.getType(metadata.getDatabaseProductName()).isMySqlFamily()
        && !metadata.supportsCatalogsInTableDefinitions()
        && !metadata.supportsCatalogsInDataManipulation()) {
      return "";
    }
    return catalog;
  }

  static String metadataSchema(DatabaseMetaData metaData, String schemaPattern, boolean exactSchema)
      throws SQLException {
    if (schemaPattern != null && (exactSchema || !schemaPattern.contains("%")
        && !schemaPattern.contains("_") && !schemaPattern.contains("\\"))) {
      return schemaPattern;
    }
    try {
      if (!metaData.supportsSchemasInTableDefinitions()
          && !metaData.supportsSchemasInDataManipulation()) {
        return "";
      }
      String schema = metaData.getConnection().getSchema();
      return schema == null ? "" : schema;
    } catch (SQLFeatureNotSupportedException | AbstractMethodError ex) {
      // AbstractMethodError: pre-JDBC 4.1 drivers do not implement Connection.getSchema().
      return "";
    }
  }

  /**
   * Used for detecting RDBMS type and DB specific handling
   */

  public enum KeyStrategy {
    INFORMATION_SCHEMA, ORACLE, SNOWFLAKE, JDBC
  }

  public enum DatabaseSpecific {
    // --
    /**
     * 
     */
    ORACLE("ORACLE", new String[] {"SYNONYM", "TABLE", "VIEW"},
        new String[] {"SYS", "CTXSYS", "CTXAPP", "MDSYS"},
        "SELECT SYS_CONTEXT('USERENV', 'DB_NAME') AS database_name , SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') AS current_schema FROM dual"),
    // --
    // Redshift must precede PostgreSQL for drivers advertising PostgreSQL compatibility.
    REDSHIFT(
        "REDSHIFT", null, new String[] {"INFORMATION_SCHEMA", "PG_CATALOG", "PG_INTERNAL",
            "PG_TOAST", "PG_AUTOMV", "CATALOG_HISTORY"},
        "SELECT current_database(), current_schema()"),
    // --
    POSTGRESQL("POSTGRESQL",
        new String[] {"TABLE", "VIEW", "FOREIGN TABLE", "MATERIALIZED VIEW", "PARTITIONED TABLE",
            "SYSTEM TABLE", "TEMPORARY TABLE", "TEMPORARY VIEW"},
        null, "SELECT current_database(), current_schema()"),
    // --
    MSSQL("MICROSOFT SQL SERVER", new String[] {"SYSTEM TABLE", "TABLE", "VIEW"}, null,
        "SELECT DB_NAME(), SCHEMA_NAME()"),
    // --
    // MariaDB must precede MySQL for product strings mentioning both names.
    MARIADB("MARIADB", null,
        new String[] {"INFORMATION_SCHEMA", "MYSQL", "PERFORMANCE_SCHEMA", "SYS"},
        "SELECT DATABASE(), DATABASE()"),
    // --
    MYSQL("MYSQL", new String[] {"TABLE", "VIEW"}, null, "SELECT DATABASE(), DATABASE()"),
    // --
    SNOWFLAKE("SNOWFLAKE", new String[] {"TABLE", "VIEW"}, null,
        "SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()"),
    // --
    // Keep all JDBC-reported table types, including vendor-specific views and aliases.
    SAP_HANA("HANA", null, new String[] {"SYS", "SYS_DATABASES", "_SYS_STATISTICS", "_SYS_REPO"},
        "SELECT DATABASE_NAME, CURRENT_SCHEMA FROM SYS.M_DATABASE", "HDB"),
    // Teradata databases are exposed as JDBC schemas; there is no catalog qualifier.
    TERADATA("TERADATA", null, new String[] {"DBC", "SYS_CALENDAR"}, "SELECT '', DATABASE"),
    // --
    DB2("DB2", null,
        new String[] {"SYSCAT", "SYSIBM", "SYSIBMADM", "SYSSTAT", "SYSFUN", "SYSPROC", "SYSPUBLIC",
            "SYSIBMINTERNAL", "SYSIBMTS", "SYSTOOLS", "NULLID", "SQLJ"},
        "SELECT RTRIM(CURRENT SERVER), RTRIM(CURRENT SCHEMA) FROM SYSIBM.SYSDUMMY1"),
    // BigQuery maps projects to catalogs and datasets to schemas. The default dataset may be null.
    BIGQUERY("BIGQUERY", null, new String[] {"INFORMATION_SCHEMA"},
        "SELECT COALESCE(@@dataset_project_id, @@project_id), @@dataset_id", "BIG QUERY"),
    // The Databricks JDBC driver reports SparkSQL, not Databricks.
    DATABRICKS("DATABRICKS", null, new String[] {"INFORMATION_SCHEMA"},
        "SELECT current_catalog(), current_schema()", "SPARKSQL", "SPARK SQL"),
    // --
    DUCKDB("DUCK", null, null, "SELECT current_catalog(), current_schema()"),
    // --
    H2("H2", null, new String[] {"INFORMATION_SCHEMA"},
        "SELECT current_catalog(), current_schema()"),
    // --
    SQLITE("SQLITE", null, null, null), DERBY("DERBY", null, null, null), INFORMIX("INFORMIX", null,
        null, null), OTHER("OTHER", null, null, "SELECT current_database(), current_schema()");

    /** Whether MySQL-compatible catalog and INFORMATION_SCHEMA conventions apply. */
    public boolean isMySqlFamily() {
      return this == MYSQL || this == MARIADB;
    }

    public KeyStrategy getKeyStrategy() {
      switch (this) {
        case POSTGRESQL:
        case MYSQL:
        case MARIADB:
        case MSSQL:
        case H2:
        case DUCKDB:
          return KeyStrategy.INFORMATION_SCHEMA;
        case SNOWFLAKE:
          return KeyStrategy.SNOWFLAKE;
        case ORACLE:
          return KeyStrategy.ORACLE;
        default:
          return KeyStrategy.JDBC;
      }
    }

    String identString;
    private final String[] productAliases;
    String currentSchemaQuery;
    String[] tableTypes;
    String[] excludedSchemas;

    /**
     * DB specific "configuration" for extracting metadata through JDBC connection
     * 
     * @param identString an unique string identifying this DB type - will be compared to
     *        {@link java.sql.DatabaseMetaData#getDatabaseProductName()} value to identify the DB
     *        specific variant.
     * @param tableTypes which table types are considered when processing DB's schema to extract
     *        metadata information used for parsing&analyzing SQL statement (usually TABLE, VIEW).
     *        If null then all types considered.
     * @param excludedSchemas which schemas should be excluded/ignored when processing particular
     *        DB's catalog&schemas. If null, then all schemas accepted.
     * @param schemaQuery query to execute against particular DB type to get information about
     *        current catalog/db & schema.
     * @param productAliases alternative product names reported by JDBC drivers
     */
    DatabaseSpecific(String identString, String[] tableTypes, String[] excludedSchemas,
        String schemaQuery, String... productAliases) {
      this.identString = identString;
      this.productAliases = productAliases;
      this.tableTypes = tableTypes;
      this.excludedSchemas = excludedSchemas;
      this.currentSchemaQuery = schemaQuery;
    }

    public static DatabaseSpecific getType(String productName) {
      if (productName == null) {
        return OTHER;
      }
      final String name = productName.toUpperCase(Locale.ROOT);
      for (DatabaseSpecific type : values()) {
        if (name.contains(type.identString)) {
          return type;
        }
        for (String alias : type.productAliases) {
          if (name.contains(alias)) {
            return type;
          }
        }
      }
      return OTHER;
    }

    /**
     * PostgreSQL metadata must not use speculative SQL: an error aborts a caller transaction. Its
     * JDBC driver also supplies vendor-specific column types and comments. Databases without
     * INFORMATION_SCHEMA also use JDBC directly. MySQL/MariaDB use JDBC to preserve the driver
     * mapping of databases to catalogs or schemas. Snowflake and Databricks enumerate JDBC catalogs
     * explicitly to avoid current-catalog-only INFORMATION_SCHEMA results.
     *
     * @return whether extraction should use JDBC metadata directly
     */
    public boolean usesJdbcMetadata() {
      return this == POSTGRESQL || isMySqlFamily() || usesCatalogScopedJdbcMetadata()
          || !supportsInformationSchema();
    }

    /** Profiles whose JDBC discovery must enumerate each catalog explicitly. */
    boolean usesCatalogScopedJdbcMetadata() {
      return this == SNOWFLAKE || this == DATABRICKS;
    }

    /** Whether INFORMATION_SCHEMA queries may be used, including guarded probes for OTHER. */
    public boolean supportsInformationSchema() {
      switch (this) {
        case ORACLE:
        case DB2:
        case SQLITE:
        case DERBY:
        case INFORMIX:
          return false;
        default:
          return true;
      }
    }

    public String getCurrentSchemaQuery() {
      return this.currentSchemaQuery;
    }

    /**
     * Filtering out certain schemas(usually system)
     * 
     * @param schema
     * @return true if the passed-
     */
    public boolean processSchema(String schema) {
      if (excludedSchemas != null) {
        for (String itm : excludedSchemas) {
          if (schema.equalsIgnoreCase(itm)) {
            return false;
          }
        }
      }
      return true;
    }

    public String[] getTableTypes() {
      return tableTypes;
    }
  }

  /**
   * Safe variant of java.sql.ResultSet.findColumn() Does not throw SQLException if columnName does
   * not exist in result set.
   * 
   * @param rs
   * @param columnName
   * @return index of the searched column in the results set or -1 if not found
   */
  public static int findColumnSafe(ResultSet rs, String columnName) {
    try {
      return rs.findColumn(columnName);
    } catch (SQLException e) {
      return -1;
    }
  }

  /**
   * Retrieves column's value from ResultSet safely (does not throw SQLException if column (name)
   * not present in ResultSet.
   * 
   * @param rs
   * @param columnName
   * @return column's value or NULL if column not found
   */
  static String getStringSafe(ResultSet rs, String columnName) {
    try {
      return rs.getString(columnName);
    } catch (SQLException e) {
      return null;
    }
  }

  /**
   * Retrieves column's value from ResultSet safely (does not throw SQLException if column (name)
   * not present in ResultSet.
   * 
   * @param rs
   * @param columnName
   * @param defaultValue
   * @return column's value or passed-in defaultValue if column not found or has NULL value
   */
  static String getStringSafe(ResultSet rs, String columnName, String defaultValue) {
    try {
      final String val = rs.getString(columnName);
      return val != null ? val : defaultValue;
    } catch (SQLException e) {
      return defaultValue;
    }
  }

  static String getStringSafe(ResultSet rs, int columnIdx, String defaultValue) {
    try {
      final String val = rs.getString(columnIdx);
      return val != null ? val : defaultValue;
    } catch (SQLException e) {
      return defaultValue;
    }
  }


  static Integer getIntSafe(ResultSet rs, String columnName) {
    try {
      int value = rs.getInt(columnName);
      return rs.wasNull() ? null : value;
    } catch (SQLException e) {
      return null;
    }
  }

  static Short getShortSafe(ResultSet rs, String columnName) {
    try {
      short value = rs.getShort(columnName);
      return rs.wasNull() ? null : value;
    } catch (SQLException e) {
      return null;
    }
  }

  static Boolean getBooleanSafe(ResultSet rs, String columnName) {
    try {
      return rs.getBoolean(columnName);
    } catch (SQLException e) {
      return null;
    }
  }

  /**
   * Escapes potential SQL's wildcard characters collisions in input string using provided escape
   * char. e.g. "TABLEAB_C" to "TABLEAB/_C" to treat it as plain string, not wildcard
   * 
   * 
   * @param input string to escape
   * @param escapeChar character to use to escape (usually obtained through calling
   *        DatabaseMetadata.getSearchStringEscape() )
   * @return
   */
  public static final String escapeSQLWildcardChars(String input, String escapeChar) {
    final String escape = escapeChar.equals("\\") ? "\\\\" : escapeChar;
    return input.replaceAll("([_%" + escape + "])", escape + "$1");
  }
}
