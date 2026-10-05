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

import java.util.Objects;
import java.util.TreeMap;

public class JdbcIndex {

  String tableCatalog;
  String tableSchema;
  String tableName;
  Boolean nonUnique;
  String indexQualifier;
  String indexName;
  Short type;

  TreeMap<Short, JdbcIndexColumn> columns = new TreeMap<>();

  public JdbcIndex(String tableCatalog, String tableSchema, String tableName, Boolean nonUnique,
      String indexQualifier, String indexName, Short type) {
    this.tableCatalog = tableCatalog;
    this.tableSchema = tableSchema;
    this.tableName = tableName;
    this.nonUnique = nonUnique;
    this.indexQualifier = indexQualifier;
    this.indexName = indexName;
    this.type = type;
  }

  public JdbcIndexColumn put(Short ordinalPosition, String columnName, String ascOrDesc,
      Long cardinality, Long pages, String filterCondition) {
    JdbcIndexColumn column = new JdbcIndexColumn(ordinalPosition, columnName, ascOrDesc,
        cardinality, pages, filterCondition);
    return columns.put(ordinalPosition, column);
  }

  public String getTableCatalog() {
    return tableCatalog;
  }

  public String getTableSchema() {
    return tableSchema;
  }

  public String getTableName() {
    return tableName;
  }

  public Boolean getNonUnique() {
    return nonUnique;
  }

  public String getIndexQualifier() {
    return indexQualifier;
  }

  public String getIndexName() {
    return indexName;
  }

  public Short getType() {
    return type;
  }

  public java.util.Map<Short, JdbcIndexColumn> getColumns() {
    return java.util.Collections.unmodifiableMap(columns);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof JdbcIndex)) {
      return false;
    }
    JdbcIndex that = (JdbcIndex) other;
    return Objects.equals(tableCatalog, that.tableCatalog)
        && Objects.equals(tableSchema, that.tableSchema)
        && Objects.equals(tableName, that.tableName) && Objects.equals(nonUnique, that.nonUnique)
        && Objects.equals(indexQualifier, that.indexQualifier)
        && Objects.equals(indexName, that.indexName) && Objects.equals(type, that.type)
        && Objects.equals(columns, that.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableCatalog, tableSchema, tableName, nonUnique, indexQualifier, indexName,
        type, columns);
  }
}
