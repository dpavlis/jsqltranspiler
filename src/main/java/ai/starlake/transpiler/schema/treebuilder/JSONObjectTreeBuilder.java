/**
 * Starlake.AI JSQLTranspiler is a SQL to DuckDB Transpiler.
 * Copyright (C) 2024 Starlake.AI <hayssam.saleh@starlake.ai>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.starlake.transpiler.schema.treebuilder;

import ai.starlake.transpiler.JSQLColumResolver;
import ai.starlake.transpiler.schema.JdbcColumn;
import ai.starlake.transpiler.schema.JdbcMetaData;
import ai.starlake.transpiler.schema.JdbcResultSetMetaData;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Select;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Converts resolved SQL data lineage into JSONObject representation
 * 
 * @see ai.starlake.transpiler.schema.treebuilder.JsonTreeBuilder
 * 
 * 
 */
public class JSONObjectTreeBuilder extends TreeBuilder<JSONObject> {
  private JSQLColumResolver resolver;

  public JSONObjectTreeBuilder(JdbcResultSetMetaData resultSetMetaData) {
    super(resultSetMetaData);
  }

  @SuppressWarnings({"PMD.CyclomaticComplexity"})
  private JSONObject convertNodeToJson(JdbcColumn column, String alias) {
    JSONObject json = new JSONObject();

    json.put("name", column.columnName);

    if (alias != null && !alias.isEmpty()) {
      json.put("alias", alias);
    }

    if (column.getExpression() instanceof Column) {
      json.put("table", JSQLColumResolver.getQualifiedTableName(column.tableCatalog,
          column.tableSchema, column.tableName));

      if (column.scopeTable != null && !column.scopeTable.isEmpty()) {
        json.put("scope", JSQLColumResolver.getQualifiedColumnName(column.scopeCatalog,
            column.scopeSchema, column.scopeTable, column.scopeColumn));
      }

      json.put("dataType", "java.sql.Types." + JdbcMetaData.getTypeName(column.dataType));
      json.put("typeName", column.typeName);
      json.put("columnSize", column.columnSize);
      json.put("decimalDigits", column.decimalDigits);
      json.put("nullable", column.isNullable.equalsIgnoreCase("YES"));
    }

    Expression expression = column.getExpression();
    if (expression instanceof Select) {
      Select select = (Select) expression;
      try {
        json.put("subquery", resolver.getLineage(this.getClass(), select));
      } catch (NoSuchMethodException | InvocationTargetException | InstantiationException
          | IllegalAccessException | SQLException e) {
        throw new RuntimeException(e);
      }
    } else if (!column.getChildren().isEmpty()) {

      JSONArray columnSetJson = new JSONArray();
      for (JdbcColumn child : column.getChildren()) {
        columnSetJson.put(convertNodeToJson(child, ""));
      }
      json.append("columnSet", columnSetJson);
    }
    return json;
  }

  @Override
  public JSONObject getConvertedTree(JSQLColumResolver resolver) throws SQLException {
    this.resolver = resolver;
    JSONObject json = new JSONObject();
    JSONArray columnSet = new JSONArray();

    for (int i = 0; i < resultSetMetaData.getColumnCount(); i++) {
      columnSet.put(convertNodeToJson(resultSetMetaData.getColumns().get(i),
          resultSetMetaData.getLabels().get(i)));
    }
    json.put("columnSet", columnSet);

    return json;
  }
}
