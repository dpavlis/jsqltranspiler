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

import java.util.LinkedList;
import java.util.Objects;

public class JdbcPrimaryKey {

  String tableCatalog;
  String tableSchema;
  String tableName;
  String primaryKeyName;

  LinkedList<String> columnNames = new LinkedList<>();

  public JdbcPrimaryKey(String tableCatalog, String tableSchema, String tableName,
      String primaryKeyName) {
    this.tableCatalog = tableCatalog;
    this.tableSchema = tableSchema;
    this.tableName = tableName;
    this.primaryKeyName = primaryKeyName;
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

  public String getPrimaryKeyName() {
    return primaryKeyName;
  }

  public java.util.List<String> getColumnNames() {
    return java.util.Collections.unmodifiableList(columnNames);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof JdbcPrimaryKey)) {
      return false;
    }
    JdbcPrimaryKey that = (JdbcPrimaryKey) other;
    return Objects.equals(tableCatalog, that.tableCatalog)
        && Objects.equals(tableSchema, that.tableSchema)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(primaryKeyName, that.primaryKeyName)
        && Objects.equals(columnNames, that.columnNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableCatalog, tableSchema, tableName, primaryKeyName, columnNames);
  }
}
