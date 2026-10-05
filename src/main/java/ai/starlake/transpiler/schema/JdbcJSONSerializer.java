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

import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Comparator;
import java.sql.DatabaseMetaData;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

public class JdbcJSONSerializer {

  public static void toJson(JdbcMetaData metadata, Writer out, int indent) {
    toJson(metadata).write(out, indent, 0);
  }

  public static void toJson(JdbcMetaData metadata, Writer out) {
    toJson(metadata).write(out);
  }

  public static JSONObject toJson(JdbcMetaData metadata) {
    JSONObject metadataObject = new JSONObject();
    metadataObject.put("databaseType", metadata.getDatabaseType());
    metadataObject.put("currentCatalog", metadata.getCurrentCatalogName());
    metadataObject.put("currentSchema", metadata.getCurrentSchemaName());
    metadataObject.put("catalogSeparator", metadata.getCatalogSeparator());

    JSONArray catalogsArray = new JSONArray();

    for (JdbcCatalog catalog : metadata.getCatalogsList()) {
      catalogsArray.put(toJson(catalog, metadata.getExtractionOptions()));
    }
    metadataObject.put("catalogs", catalogsArray);

    return metadataObject;

  }

  public static JdbcMetaData fromJson(Reader in) {
    JSONObject json = new JSONObject(new JSONTokener(in));

    JdbcMetaData metadata =
        new JdbcMetaData(json.getString("currentCatalog"), json.getString("currentSchema"));
    metadata.setDatabaseType(json.getString("databaseType"));
    metadata.setCatalogSeparator(json.getString("catalogSeparator"));

    JSONArray jsonCatalogs = json.getJSONArray("catalogs");

    List<JdbcCatalog> catalogs = new ArrayList<JdbcCatalog>(jsonCatalogs.length());
    for (int i = 0; i < jsonCatalogs.length(); i++) {
      catalogs.add(fromJsonCatalog(jsonCatalogs.getJSONObject(i)));
    }

    // The current database need not be a JDBC catalog (notably on Oracle).
    metadata.clear();
    metadata.setCatalogsList(catalogs);
    restoreScopes(metadata);
    metadata.normalizeCurrentCatalog(false);
    return metadata;

  }

  protected static JSONObject toJson(JdbcCatalog catalog) {
    return toJson(catalog, JdbcMetaDataOptions.defaults().setIndices(true));
  }

  private static JSONObject toJson(JdbcCatalog catalog, JdbcMetaDataOptions options) {
    JSONObject catalogObject = new JSONObject();
    catalogObject.put("name", catalog.tableCatalog);
    catalogObject.put("separator", catalog.catalogSeparator);

    JSONArray schemasArray = new JSONArray();

    for (JdbcSchema schema : catalog.schemas.values()) {
      schemasArray.put(toJson(schema, options));
    }
    catalogObject.put("schemas", schemasArray);

    return catalogObject;
  }

  protected static JSONObject toJson(JdbcSchema schema) {
    return toJson(schema, JdbcMetaDataOptions.defaults().setIndices(true));
  }

  private static JSONObject toJson(JdbcSchema schema, JdbcMetaDataOptions options) {
    JSONObject schemaObject = new JSONObject();
    schemaObject.put("name", schema.tableSchema);

    JSONArray tablesArray = new JSONArray();

    for (JdbcTable table : schema.tables.values()) {
      tablesArray.put(toJson(table, options));
    }

    schemaObject.put("tables", tablesArray);

    return schemaObject;

  }

  protected static JSONObject toJson(JdbcTable table) {
    return toJson(table, JdbcMetaDataOptions.defaults().setIndices(true));
  }

  private static JSONObject toJson(JdbcTable table, JdbcMetaDataOptions options) {
    JSONObject tableObject = new JSONObject();
    tableObject.put("name", table.getTableName());
    tableObject.put("type", table.getTableType());

    JSONArray columnsArray = new JSONArray();

    List<JdbcColumn> sorted = new ArrayList<>(table.columns.values());
    sorted.sort(Comparator.comparing(column -> column.ordinalPosition,
        Comparator.nullsLast(Comparator.naturalOrder())));
    for (JdbcColumn column : sorted) {
      columnsArray.put(toJson(column, options));
    }

    tableObject.put("columns", columnsArray);
    if (options.comments) {
      putText(tableObject, "remarks", table.remarks);
    }
    if (options.primaryKeys && table.primaryKey != null) {
      JSONObject key = new JSONObject();
      putText(key, "name", table.primaryKey.primaryKeyName);
      key.put("columns", new JSONArray(table.primaryKey.columnNames));
      tableObject.put("primaryKey", key);
    }
    if (options.foreignKeys && !table.foreignKeys.isEmpty()) {
      JSONArray keys = new JSONArray();
      for (JdbcReference reference : table.foreignKeys) {
        JSONObject key = new JSONObject();
        putText(key, "name", reference.fkName);
        key.put("referencedCatalog", reference.pkTableCatalog);
        key.put("referencedSchema", reference.pkTableSchema);
        key.put("referencedTable", reference.pkTableName);
        key.put("updateRule", JdbcKeyExtractor.ruleName(reference.updateRule));
        key.put("deleteRule", JdbcKeyExtractor.ruleName(reference.deleteRule));
        putText(key, "referencedKey", reference.pkName);
        key.put("deferrability", reference.deferrability);
        JSONArray fkColumns = new JSONArray();
        JSONArray pkColumns = new JSONArray();
        for (String[] pair : reference.columns) {
          fkColumns.put(pair[0]);
          pkColumns.put(pair[1]);
        }
        key.put("columns", fkColumns);
        key.put("referencedColumns", pkColumns);
        keys.put(key);
      }
      tableObject.put("foreignKeys", keys);
    }
    if (options.indices && !table.indices.isEmpty()) {
      JSONArray indices = new JSONArray();
      for (JdbcIndex index : table.indices.values()) {
        JSONObject json = new JSONObject();
        putText(json, "name", index.indexName);
        if (index.nonUnique != null) {
          json.put("unique", !index.nonUnique);
        }
        json.put("type", index.type);
        json.put("qualifier", index.indexQualifier);
        JSONArray names = new JSONArray();
        JSONArray details = new JSONArray();
        for (JdbcIndexColumn column : index.columns.values()) {
          names.put(column.columnName == null ? JSONObject.NULL : column.columnName);
          JSONObject detail = new JSONObject();
          detail.put("ordinalPosition", column.ordinalPosition);
          detail.put("ascOrDesc", column.ascOrDesc);
          detail.put("cardinality", column.cardinality);
          detail.put("pages", column.pages);
          detail.put("filterCondition", column.filterCondition);
          details.put(detail);
        }
        json.put("columns", names);
        json.put("columnDetails", details);
        indices.put(json);
      }
      tableObject.put("indices", indices);
    }
    return tableObject;
  }

  protected static JSONObject toJson(JdbcColumn column) {
    return toJson(column, JdbcMetaDataOptions.defaults().setIndices(true));
  }

  private static JSONObject toJson(JdbcColumn column, JdbcMetaDataOptions options) {
    JSONObject columnObject = new JSONObject();
    columnObject.put("name", column.columnName);
    columnObject.put("type", column.typeName);
    columnObject.put("typeID", column.dataType);
    columnObject.put("size", column.columnSize);
    if (column.decimalDigits != null) {
      columnObject.put("decimalDigits", column.decimalDigits);
    }
    putBoolean(columnObject, "isNullable", column.isNullable);
    if (options.comments) {
      putText(columnObject, "remarks", column.remarks);
    }
    if (options.columnDetails) {
      if (column.ordinalPosition != null && column.ordinalPosition > 0) {
        columnObject.put("ordinalPosition", column.ordinalPosition);
      }
      putText(columnObject, "default", column.columnDefinition);
      putBoolean(columnObject, "autoIncrement", column.isAutomaticIncrement);
      putBoolean(columnObject, "generated", column.isGeneratedColumn);
    }

    return columnObject;
  }

  protected static JdbcCatalog fromJsonCatalog(JSONObject json) {
    final String catalogName = json.getString("name");
    JdbcCatalog catalog = new JdbcCatalog(catalogName, json.getString("separator"));

    JSONArray jsonSchemas = json.getJSONArray("schemas");

    List<JdbcSchema> schemas = new ArrayList<JdbcSchema>(jsonSchemas.length());
    for (int i = 0; i < jsonSchemas.length(); i++) {
      schemas.add(fromJsonSchema(jsonSchemas.getJSONObject(i)));
    }
    catalog.setSchemas(schemas);
    return catalog;
  }

  protected static JdbcSchema fromJsonSchema(JSONObject json) {
    JdbcSchema schema = new JdbcSchema();
    schema.setSchemaName(json.getString("name"));

    JSONArray jsonTables = json.getJSONArray("tables");

    List<JdbcTable> tables = new ArrayList<JdbcTable>(jsonTables.length());
    for (int i = 0; i < jsonTables.length(); i++) {
      tables.add(fromJsonTable(jsonTables.getJSONObject(i)));
    }

    schema.setTables(tables);

    return schema;

  }

  protected static JdbcTable fromJsonTable(JSONObject json) {
    JdbcTable table = new JdbcTable();
    table.setTableName(json.getString("name"));
    table.setTableType(json.getString("type"));

    JSONArray jsonColumns = json.getJSONArray("columns");

    List<JdbcColumn> columns = new ArrayList<JdbcColumn>(jsonColumns.length());
    for (int i = 0; i < jsonColumns.length(); i++) {
      columns.add(fromJsonColumn(jsonColumns.getJSONObject(i)));
    }

    table.setColumns(columns);
    table.remarks = json.optString("remarks", null);
    JSONObject pk = json.optJSONObject("primaryKey");
    if (pk != null) {
      table.primaryKey =
          new JdbcPrimaryKey(null, null, table.tableName, pk.optString("name", null));
      JSONArray names = pk.getJSONArray("columns");
      for (int i = 0; i < names.length(); i++) {
        table.primaryKey.columnNames.add(names.getString(i));
      }
    }
    JSONArray references = json.optJSONArray("foreignKeys");
    if (references != null) {
      for (int i = 0; i < references.length(); i++) {
        JSONObject key = references.getJSONObject(i);
        JdbcReference reference = new JdbcReference(key.optString("referencedCatalog", null),
            key.optString("referencedSchema", null), key.getString("referencedTable"), null, null,
            table.tableName, JdbcKeyExtractor.rule(key.optString("updateRule", null)),
            JdbcKeyExtractor.rule(key.optString("deleteRule", null)), key.optString("name", null),
            key.optString("referencedKey", null),
            key.has("deferrability") ? (short) key.getInt("deferrability") : null);
        JSONArray fkColumns = key.getJSONArray("columns");
        JSONArray pkColumns = key.getJSONArray("referencedColumns");
        if (fkColumns.length() != pkColumns.length()) {
          throw new IllegalArgumentException("Foreign key columns must be paired");
        }
        for (int j = 0; j < fkColumns.length(); j++) {
          reference.columns.add(new String[] {fkColumns.getString(j), pkColumns.getString(j)});
        }
        table.foreignKeys.add(reference);
      }
    }
    JSONArray indices = json.optJSONArray("indices");
    if (indices != null) {
      for (int i = 0; i < indices.length(); i++) {
        JSONObject key = indices.getJSONObject(i);
        JdbcIndex index = new JdbcIndex(null, null, table.tableName,
            key.has("unique") ? !key.getBoolean("unique") : null, key.optString("qualifier", null),
            key.getString("name"), key.has("type") ? (short) key.getInt("type") : null);
        JSONArray names = key.getJSONArray("columns");
        JSONArray details = key.optJSONArray("columnDetails");
        for (int j = 0; j < names.length(); j++) {
          JSONObject detail =
              details != null && j < details.length() ? details.getJSONObject(j) : new JSONObject();
          index.put((short) detail.optInt("ordinalPosition", j + 1),
              names.isNull(j) ? null : names.getString(j), detail.optString("ascOrDesc", null),
              detail.has("cardinality") ? detail.getLong("cardinality") : null,
              detail.has("pages") ? detail.getLong("pages") : null,
              detail.optString("filterCondition", null));
        }
        table.indices.put(index.indexName, index);
      }
    }
    return table;

  }

  protected static JdbcColumn fromJsonColumn(JSONObject json) {
    JdbcColumn column = new JdbcColumn(json.getString("name"));
    column.typeName = json.getString("type");
    column.dataType = json.getInt("typeID");
    column.columnSize = json.has("size") ? json.getInt("size") : null;
    column.isNullable = readBoolean(json, "isNullable");
    column.nullable = "YES".equals(column.isNullable) ? DatabaseMetaData.columnNullable
        : "NO".equals(column.isNullable) ? DatabaseMetaData.columnNoNulls
            : DatabaseMetaData.columnNullableUnknown;
    column.remarks = json.optString("remarks", null);
    column.columnDefinition = json.optString("default", null);
    column.ordinalPosition = json.has("ordinalPosition") ? json.getInt("ordinalPosition") : null;
    column.isAutomaticIncrement = readBoolean(json, "autoIncrement");
    column.isGeneratedColumn = readBoolean(json, "generated");
    column.decimalDigits = json.has("decimalDigits") ? json.getInt("decimalDigits") : null;

    return column;
  }

  private static void putText(JSONObject json, String key, String value) {
    if (value != null && !value.isEmpty()) {
      json.put(key, value);
    }
  }

  private static void putBoolean(JSONObject json, String key, String value) {
    if ("YES".equalsIgnoreCase(value) || "NO".equalsIgnoreCase(value)) {
      json.put(key, "YES".equalsIgnoreCase(value));
    }
  }

  private static String readBoolean(JSONObject json, String key) {
    return !json.has(key) || json.isNull(key) ? "" : json.getBoolean(key) ? "YES" : "NO";
  }

  private static void restoreScopes(JdbcMetaData metadata) {
    for (JdbcCatalog catalog : metadata.getCatalogsList()) {
      for (JdbcSchema schema : catalog.schemas.values()) {
        schema.tableCatalog = catalog.tableCatalog;
        for (JdbcTable table : schema.tables.values()) {
          table.tableCatalog = catalog.tableCatalog;
          table.tableSchema = schema.tableSchema;
          for (JdbcColumn column : table.columns.values()) {
            column.tableCatalog = table.tableCatalog;
            column.tableSchema = table.tableSchema;
            column.scopeCatalog = table.tableCatalog;
            column.scopeSchema = table.tableSchema;
            column.scopeTable = table.tableName;
            column.scopeColumn = column.columnName;
          }
          if (table.primaryKey != null) {
            table.primaryKey.tableCatalog = table.tableCatalog;
            table.primaryKey.tableSchema = table.tableSchema;
          }
          for (JdbcReference reference : table.foreignKeys) {
            reference.fkTableCatalog = table.tableCatalog;
            reference.fkTableSchema = table.tableSchema;
          }
          for (JdbcIndex index : table.indices.values()) {
            index.tableCatalog = table.tableCatalog;
            index.tableSchema = table.tableSchema;
          }
        }
      }
    }
  }

}
