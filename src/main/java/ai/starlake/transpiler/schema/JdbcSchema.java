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

import net.sf.jsqlparser.schema.Table;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.logging.Logger;

public class JdbcSchema implements Comparable<JdbcSchema> {

  public static final Logger LOGGER = Logger.getLogger(JdbcSchema.class.getName());

  public String tableSchema;
  public String tableCatalog;

  public CaseInsensitiveLinkedHashMap<JdbcTable> tables = new CaseInsensitiveLinkedHashMap<>();
  public CaseInsensitiveLinkedHashMap<JdbcTable> synonyms = new CaseInsensitiveLinkedHashMap<>();
  public CaseInsensitiveLinkedHashMap<JdbcTable> droppedTables =
      new CaseInsensitiveLinkedHashMap<>();

  public JdbcSchema(String tableSchema, String tableCatalog) {
    this.tableSchema = tableSchema != null ? tableSchema : "";
    this.tableCatalog = tableCatalog != null ? tableCatalog : "";
  }

  public JdbcSchema() {}

  public static Collection<JdbcSchema> getSchemasFromInformationSchema(Connection conn)
      throws SQLException {
    DatabaseMetaData metaData = conn.getMetaData();
    if (JdbcUtils.DatabaseSpecific.getType(metaData.getDatabaseProductName()).usesJdbcMetadata()) {
      return getSchemas(metaData);
    }
    return JdbcUtils.metadataProbe(conn, () -> readInformationSchema(conn));
  }

  private static Collection<JdbcSchema> readInformationSchema(Connection conn) throws SQLException {
    String defaultCatalog = JdbcUtils.metadataCatalog(conn.getMetaData(), null);
    ArrayList<JdbcSchema> jdbcSchemas = new ArrayList<>();

    String sqlStr =
        String.format("SELECT * FROM %s.information_schema.schemata", conn.getCatalog());
    try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sqlStr)) {

      while (rs.next()) {
        // TABLE_SCHEM String => schema name
        String tableSchema = JdbcUtils.getStringSafe(rs, "SCHEMA_NAME");
        // TABLE_CATALOG String => catalog name (maybe null)
        String tableCatalog = JdbcUtils.getStringSafe(rs, "CATALOG_NAME", defaultCatalog);
        if (tableSchema != null && !tableSchema.isBlank()) {
          JdbcSchema jdbcSchema = new JdbcSchema(tableSchema, tableCatalog);
          jdbcSchemas.add(jdbcSchema);
        }
      }
      // add <empty> schema as some DBs don't have the concept of schema for tables
      jdbcSchemas.add(new JdbcSchema("", ""));

    }
    return jdbcSchemas;
  }

  /** Catalog-scoped discovery; unscoped fallback must not invent ownership. */
  static Collection<JdbcSchema> getSchemas(DatabaseMetaData metaData, Collection<String> catalogs)
      throws SQLException {
    return getSchemas(metaData, catalogs, List.of(), catalogs);
  }

  static Collection<JdbcSchema> getSchemas(DatabaseMetaData metaData, Collection<String> catalogs,
      List<String[]> patterns, Collection<String> visibleCatalogs) throws SQLException {
    List<JdbcSchema> schemas = new ArrayList<>();
    try {
      for (String catalog : catalogs) {
        Set<String> schemaPatterns = new java.util.LinkedHashSet<>();
        if (patterns.isEmpty()) {
          schemaPatterns.add(null);
        } else {
          for (String[] pattern : patterns) {
            if (pattern[0] == null || catalog.equalsIgnoreCase(pattern[0])) {
              String escape = metaData.getSearchStringEscape();
              schemaPatterns
                  .add(escape == null || escape.isEmpty() || "\\".equals(escape) ? pattern[1]
                      : pattern[1].replace("\\", escape));
            }
          }
        }
        for (String schemaPattern : schemaPatterns) {
          try (ResultSet rs = metaData.getSchemas(catalog, schemaPattern)) {
            readJdbcSchemas(rs, catalog, schemas);
          } catch (java.sql.SQLFeatureNotSupportedException unsupported) {
            throw unsupported;
          } catch (SQLException failure) {
            boolean visible = visibleCatalogs.stream().anyMatch(catalog::equalsIgnoreCase);
            if (visible && ("42501".equals(failure.getSQLState()) || failure.getErrorCode() == 2003
                || failure.getErrorCode() == 2043)) {
              LOGGER.fine("Skipping unreadable catalog " + catalog + ": " + failure.getMessage());
            } else {
              throw failure;
            }
          }
        }
      }
      if (!catalogs.isEmpty()) {
        return schemas.stream().distinct().collect(java.util.stream.Collectors.toList());
      }
    } catch (java.sql.SQLFeatureNotSupportedException unsupported) {
      // Discard partial scoped results and read the unscoped API only once.
      schemas.clear();
    }
    String fallbackCatalog = catalogs.size() == 1 ? catalogs.iterator().next() : null;
    try (ResultSet rs = metaData.getSchemas()) {
      readJdbcSchemas(rs, fallbackCatalog, schemas);
    }
    return schemas.stream().distinct().collect(java.util.stream.Collectors.toList());
  }

  private static void readJdbcSchemas(ResultSet rs, String fallbackCatalog,
      Collection<JdbcSchema> schemas) throws SQLException {
    while (rs.next()) {
      String schema = rs.getString("TABLE_SCHEM");
      if (schema == null || schema.isBlank()) {
        continue;
      }
      String catalog = JdbcUtils.getStringSafe(rs, "TABLE_CATALOG");
      if (catalog == null || catalog.isEmpty()) {
        if (fallbackCatalog == null) {
          throw new SQLException("Schema " + schema + " has ambiguous catalog ownership");
        }
        catalog = fallbackCatalog;
      }
      schemas.add(new JdbcSchema(schema, catalog));
    }
  }

  public static Collection<JdbcSchema> getSchemas(DatabaseMetaData metaData) throws SQLException {
    String defaultCatalog = JdbcUtils.metadataCatalog(metaData, null);
    ArrayList<JdbcSchema> jdbcSchemas = new ArrayList<>();

    if (!metaData.supportsSchemasInTableDefinitions()
        && !metaData.supportsSchemasInDataManipulation()) {
      for (JdbcCatalog catalog : JdbcCatalog.getCatalogs(metaData)) {
        jdbcSchemas.add(new JdbcSchema("", catalog.tableCatalog));
      }
      return jdbcSchemas;
    }
    try (ResultSet rs = metaData.getSchemas();) {

      while (rs.next()) {
        // TABLE_SCHEM String => schema name
        String tableSchema = JdbcUtils.getStringSafe(rs, "TABLE_SCHEM");
        // TABLE_CATALOG String => catalog name (maybe null)
        String tableCatalog = JdbcUtils.metadataCatalogValue(metaData,
            JdbcUtils.getStringSafe(rs, "TABLE_CATALOG", defaultCatalog));
        if (tableSchema != null && !tableSchema.isBlank()) {
          JdbcSchema jdbcSchema = new JdbcSchema(tableSchema, tableCatalog);
          jdbcSchemas.add(jdbcSchema);
        }
      }
      // add <empty> schema as some DBs don't have the concept of schema for tables
      jdbcSchemas.add(new JdbcSchema("", ""));

    }
    return jdbcSchemas;
  }

  public JdbcTable put(JdbcTable jdbcTable) {
    return tables.put(jdbcTable.tableName, jdbcTable);
  }

  public JdbcTable get(Table table) {
    return get(table.getUnquotedName());
  }

  public JdbcTable get(String tableName) {
    // @todo: check if this unquoting is still necessary
    String unquotedTableName = tableName.replaceAll("^\"|\"$", "");

    JdbcTable jdbcTable = tables.get(unquotedTableName);

    // Virtual tables from WITH items shadow physical tables and synonyms
    if (jdbcTable != null && jdbcTable.tableType.equalsIgnoreCase("VIRTUAL TABLE")) {
      return jdbcTable;
    } else if (droppedTables.containsKey(unquotedTableName)) {
      return null;
    } else if (synonyms.containsKey(unquotedTableName)) {
      final JdbcTable synonym = synonyms.get(unquotedTableName);
      synonym.tableName = unquotedTableName;
      return synonym;
    } else {
      return jdbcTable;
    }
  }

  @Override
  public int compareTo(JdbcSchema o) {
    int compareTo = tableCatalog.compareToIgnoreCase(o.tableCatalog);

    if (compareTo == 0) {
      compareTo = tableSchema.compareToIgnoreCase(o.tableSchema);
    }

    return compareTo;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof JdbcSchema)) {
      return false;
    }

    JdbcSchema jdbcSchema = (JdbcSchema) o;

    if (!tableSchema.equals(jdbcSchema.tableSchema)) {
      return false;
    }
    if (!Objects.equals(tableCatalog, jdbcSchema.tableCatalog)) {
      return false;
    }
    return Objects.equals(tables, jdbcSchema.tables);
  }

  @Override
  public int hashCode() {
    int result = tableSchema.hashCode();
    result = 31 * result + (tableCatalog != null ? tableCatalog.hashCode() : 0);
    result = 31 * result + (tables != null ? tables.hashCode() : 0);
    return result;
  }

  public JdbcTable put(String key, JdbcTable value) {
    return tables.put(key, value);
  }

  public boolean containsValue(JdbcTable value) {
    return tables.containsValue(value);
  }

  public int size() {
    return tables.size();
  }

  public JdbcTable replace(String key, JdbcTable value) {
    return tables.replace(key, value);
  }

  public boolean isEmpty() {
    return tables.isEmpty();
  }

  public JdbcTable compute(String key,
      BiFunction<? super String, ? super JdbcTable, ? extends JdbcTable> remappingFunction) {
    return tables.compute(key, remappingFunction);
  }

  public void putAll(Map<? extends String, ? extends JdbcTable> m) {
    tables.putAll(m);
  }

  public Collection<JdbcTable> values() {
    return tables.values();
  }

  public boolean replace(String key, JdbcTable oldValue, JdbcTable newValue) {
    return tables.replace(key, oldValue, newValue);
  }

  public void forEach(BiConsumer<? super String, ? super JdbcTable> action) {
    tables.forEach(action);
  }

  public JdbcTable getOrDefault(String key, JdbcTable defaultValue) {
    return tables.getOrDefault(key, defaultValue);
  }

  public boolean remove(String key, JdbcTable value) {
    return tables.remove(key, value);
  }

  public JdbcTable computeIfPresent(String key,
      BiFunction<? super String, ? super JdbcTable, ? extends JdbcTable> remappingFunction) {
    return tables.computeIfPresent(key, remappingFunction);
  }

  public void replaceAll(
      BiFunction<? super String, ? super JdbcTable, ? extends JdbcTable> function) {
    tables.replaceAll(function);
  }

  public JdbcTable computeIfAbsent(String key,
      Function<? super String, ? extends JdbcTable> mappingFunction) {
    return tables.computeIfAbsent(key, mappingFunction);
  }

  public JdbcTable putIfAbsent(JdbcTable value) {
    return tables.putIfAbsent(value.tableName, value);
  }

  public JdbcTable merge(String key, JdbcTable value,
      BiFunction<? super JdbcTable, ? super JdbcTable, ? extends JdbcTable> remappingFunction) {
    return tables.merge(key, value, remappingFunction);
  }

  public boolean containsKey(String key) {
    return synonyms.containsKey(key) || tables.containsKey(key);
  }

  public JdbcTable remove(String key) {
    return tables.remove(key);
  }

  public void clear() {
    tables.clear();
  }

  public Set<Map.Entry<String, JdbcTable>> entrySet() {
    return tables.entrySet();
  }

  public Set<String> keySet() {
    return tables.keySet();
  }

  /*
   * following for JSON (de)serialization
   */

  public List<JdbcTable> getTables() {
    return new ArrayList<JdbcTable>(this.tables.values());
  }

  public void setTables(List<JdbcTable> tables) {
    for (JdbcTable item : tables) {
      item.tableCatalog = this.tableCatalog;
      item.tableSchema = this.tableSchema;
      put(item);
    }
  }

  public String getSchemaName() {
    return this.tableSchema;
  }

  public void setSchemaName(String schemaName) {
    this.tableSchema = schemaName;
  }

}
