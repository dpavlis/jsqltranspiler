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

public class XmlTreeBuilder extends TreeBuilder<String> {
  private final StringBuilder xmlBuilder = new StringBuilder();
  private JSQLColumResolver resolver;

  public XmlTreeBuilder(JdbcResultSetMetaData resultSetMetaData) {
    super(resultSetMetaData);
  }

  private static String escapeAttribute(Object value) {
    return String.valueOf(value).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        .replace("\"", "&quot;").replace("'", "&apos;").replace("\n", "&#10;")
        .replace("\r", "&#13;").replace("\t", "&#9;");
  }

  private void addIndentation(int indent) {
    xmlBuilder.append("  ".repeat(Math.max(0, indent)));
  }

  private StringBuilder addIndentation(String input, int indentLevel) {
    StringBuilder result = new StringBuilder();
    String[] lines = input.split("\n");
    if (lines.length <= 1) {
      return result; // No lines or only one line which is removed
    }

    String indent = " ".repeat(indentLevel);
    // skip the first line of the XML declaration
    for (int i = 1; i < lines.length; i++) {
      result.append(indent).append(lines[i]).append("\n");
    }

    // Remove the last newline character
    if (result.length() > 0) {
      result.setLength(result.length() - 1);
    }

    return result;
  }

  @SuppressWarnings({"PMD.CyclomaticComplexity"})
  private void convertNodeToXml(JdbcColumn column, String alias, int indent) {
    addIndentation(indent);
    xmlBuilder.append("<Column");

    if (alias != null && !alias.isEmpty()) {
      xmlBuilder.append(" alias='").append(escapeAttribute(alias)).append("'");
    }
    xmlBuilder.append(" name='").append(escapeAttribute(column.columnName)).append("'");

    LineageAttributes.of(column).forEach((key, value) -> xmlBuilder.append(" ").append(key)
        .append("='").append(escapeAttribute(value)).append("'"));

    if (column.getExpression() instanceof Column) {
      xmlBuilder
          .append(" table='").append(escapeAttribute(JSQLColumResolver
              .getQualifiedTableName(column.tableCatalog, column.tableSchema, column.tableName)))
          .append("'");
      if (column.scopeTable != null && !column.scopeTable.isEmpty()) {
        xmlBuilder.append(" scope='")
            .append(escapeAttribute(JSQLColumResolver.getQualifiedColumnName(column.scopeCatalog,
                column.scopeSchema, column.scopeTable, column.scopeColumn)))
            .append("'");
      }
      xmlBuilder.append(" dataType='java.sql.Types.")
          .append(column.dataType == null ? "UNKNOWN" : JdbcMetaData.getTypeName(column.dataType))
          .append("'");
      xmlBuilder.append(" typeName='").append(escapeAttribute(column.typeName)).append("'");
      xmlBuilder.append(" columnSize='").append(escapeAttribute(column.columnSize)).append("'");
      xmlBuilder.append(" decimalDigits='").append(escapeAttribute(column.decimalDigits))
          .append("'");
      xmlBuilder.append(" nullable='").append(escapeAttribute(column.isNullable)).append("'");
    }

    Expression expression = column.getExpression();
    if (expression instanceof Select) {
      xmlBuilder.append(">\n");

      Select select = (Select) expression;
      try {
        String subquery =
            column.getSubqueryMetaData() == null ? resolver.getLineage(this.getClass(), select)
                : new XmlTreeBuilder(column.getSubqueryMetaData()).getConvertedTree(resolver);
        xmlBuilder.append(addIndentation(subquery, indent + 2)).append("\n");
      } catch (NoSuchMethodException | InvocationTargetException | InstantiationException
          | IllegalAccessException | SQLException e) {
        throw new RuntimeException(e);
      }
      addIndentation(indent);
      xmlBuilder.append("</Column>\n");
    } else if (!column.getChildren().isEmpty()) {
      xmlBuilder.append(">\n");
      addIndentation(indent + 1);
      xmlBuilder.append("<ColumnSet>\n");

      for (JdbcColumn child : column.getChildren()) {
        convertNodeToXml(child, "", indent + 2);
      }

      addIndentation(indent + 1);
      xmlBuilder.append("</ColumnSet>\n");

      addIndentation(indent);
      xmlBuilder.append("</Column>\n");
    } else {
      xmlBuilder.append("/>\n");
    }


  }

  @Override
  public String getConvertedTree(JSQLColumResolver resolver) throws SQLException {
    this.resolver = resolver;

    xmlBuilder.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    xmlBuilder.append("<ColumnSet>\n");

    for (int i = 0; i < resultSetMetaData.getColumnCount(); i++) {
      convertNodeToXml(resultSetMetaData.getColumns().get(i), resultSetMetaData.getLabels().get(i),
          1);
    }

    xmlBuilder.append("</ColumnSet>\n");
    return xmlBuilder.toString();
  }
}
