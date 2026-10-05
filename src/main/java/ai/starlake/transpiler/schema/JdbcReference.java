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

public class JdbcReference {

  String pkTableCatalog;
  String pkTableSchema;
  String pkTableName;
  String fkTableCatalog;
  String fkTableSchema;
  String fkTableName;
  Short updateRule;
  Short deleteRule;
  String fkName;
  String pkName;
  Short deferrability;

  LinkedList<String[]> columns = new LinkedList<>();

  public JdbcReference(String pkTableCatalog, String pkTableSchema, String pkTableName,
      String fkTableCatalog, String fkTableSchema, String fkTableName, Short updateRule,
      Short deleteRule, String fkName, String pkName, Short deferrability) {
    this.pkTableCatalog = pkTableCatalog;
    this.pkTableSchema = pkTableSchema;
    this.pkTableName = pkTableName;
    this.fkTableCatalog = fkTableCatalog;
    this.fkTableSchema = fkTableSchema;
    this.fkTableName = fkTableName;
    this.updateRule = updateRule;
    this.deleteRule = deleteRule;
    this.fkName = fkName;
    this.pkName = pkName;
    this.deferrability = deferrability;
  }

  public String getPkTableCatalog() {
    return pkTableCatalog;
  }

  public String getPkTableSchema() {
    return pkTableSchema;
  }

  public String getPkTableName() {
    return pkTableName;
  }

  public String getFkTableCatalog() {
    return fkTableCatalog;
  }

  public String getFkTableSchema() {
    return fkTableSchema;
  }

  public String getFkTableName() {
    return fkTableName;
  }

  public Short getUpdateRule() {
    return updateRule;
  }

  public Short getDeleteRule() {
    return deleteRule;
  }

  public String getFkName() {
    return fkName;
  }

  public String getPkName() {
    return pkName;
  }

  public Short getDeferrability() {
    return deferrability;
  }

  public java.util.List<String[]> getColumns() {
    return java.util.Collections.unmodifiableList(columns);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof JdbcReference)) {
      return false;
    }
    JdbcReference that = (JdbcReference) other;
    return Objects.equals(pkTableCatalog, that.pkTableCatalog)
        && Objects.equals(pkTableSchema, that.pkTableSchema)
        && Objects.equals(pkTableName, that.pkTableName)
        && Objects.equals(fkTableCatalog, that.fkTableCatalog)
        && Objects.equals(fkTableSchema, that.fkTableSchema)
        && Objects.equals(fkTableName, that.fkTableName)
        && Objects.equals(updateRule, that.updateRule)
        && Objects.equals(deleteRule, that.deleteRule) && Objects.equals(fkName, that.fkName)
        && Objects.equals(pkName, that.pkName) && Objects.equals(deferrability, that.deferrability)
        && java.util.Arrays.deepEquals(columns.toArray(), that.columns.toArray());
  }

  @Override
  public int hashCode() {
    return Objects.hash(pkTableCatalog, pkTableSchema, pkTableName, fkTableCatalog, fkTableSchema,
        fkTableName, updateRule, deleteRule, fkName, pkName, deferrability,
        java.util.Arrays.deepHashCode(columns.toArray()));
  }
}
