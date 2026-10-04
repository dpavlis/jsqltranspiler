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

import ai.starlake.transpiler.schema.JdbcColumn;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnalyticType;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Select;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Additive attributes shared by all lineage representations. */
public final class LineageAttributes {
  private static final Set<String> AGGREGATES = Set.of("sum", "avg", "count", "min", "max", "every",
      "any_value", "bool_and", "bool_or", "bit_and", "bit_or", "bit_xor", "array_agg", "string_agg",
      "listagg", "group_concat", "json_agg", "jsonb_agg", "json_object_agg", "jsonb_object_agg",
      "stddev", "stddev_pop", "stddev_samp", "variance", "var_pop", "var_samp", "corr", "covar_pop",
      "covar_samp", "percentile_cont", "percentile_disc", "median", "mode");
  private static final Map<String, String> LITERALS =
      Map.ofEntries(Map.entry("LongValue", "number"), Map.entry("DoubleValue", "number"),
          Map.entry("StringValue", "string"), Map.entry("DateValue", "date"),
          Map.entry("TimeValue", "time"), Map.entry("TimestampValue", "timestamp"),
          Map.entry("HexValue", "binary"), Map.entry("NullValue", "null"),
          Map.entry("BooleanValue", "boolean"));

  private LineageAttributes() {}

  private static String literalType(Expression expression) {
    if (expression == null) {
      return null;
    }
    String type = LITERALS.get(expression.getClass().getSimpleName());
    if (expression instanceof DateTimeLiteralExpression) {
      type = ((DateTimeLiteralExpression) expression).getType().name().toLowerCase(Locale.ROOT);
    } else if (expression instanceof IntervalExpression) {
      Expression operand = ((IntervalExpression) expression).getExpression();
      if (operand == null || isConstant(operand)) {
        type = "interval";
      }
    } else if (expression instanceof SignedExpression) {
      type = literalType(((SignedExpression) expression).getExpression());
    } else if (expression instanceof CastExpression) {
      CastExpression cast = (CastExpression) expression;
      if (cast.isImplicitCast() && literalType(cast.getLeftExpression()) != null) {
        if (cast.isDate()) {
          type = "date";
        } else if (cast.isTimeStamp()) {
          type = "timestamp";
        } else if (cast.isTime()) {
          type = "time";
        }
      }
    }
    return type;
  }

  private static boolean isConstant(Expression expression) {
    if (literalType(expression) != null) {
      return true;
    }
    if (expression instanceof BinaryExpression) {
      BinaryExpression binary = (BinaryExpression) expression;
      return isConstant(binary.getLeftExpression()) && isConstant(binary.getRightExpression());
    }
    if (expression instanceof Function) {
      Function function = (Function) expression;
      // JSqlParser represents INTERVAL (constant arithmetic) as an interval function operand.
      return "interval".equalsIgnoreCase(function.getName()) && function.getParameters() != null
          && function.getParameters().stream().allMatch(LineageAttributes::isConstant);
    }
    if (expression instanceof ExpressionList<?>) {
      return ((ExpressionList<?>) expression).stream().allMatch(LineageAttributes::isConstant);
    }
    return false;
  }

  public static Map<String, Object> of(JdbcColumn column) {
    Map<String, Object> attributes = new LinkedHashMap<>();
    Expression expression = column.getExpression();
    String literal = literalType(expression);
    String kind = "operator";
    if (expression instanceof Column || expression == null) {
      kind = "column";
    } else if (literal != null) {
      kind = "literal";
      attributes.put("value", expression.toString());
      attributes.put("literalType", literal);
    } else if (expression instanceof JdbcParameter) {
      kind = "parameter";
      attributes.put("index", ((JdbcParameter) expression).getIndex());
    } else if (expression instanceof JdbcNamedParameter) {
      kind = "parameter";
      attributes.put("parameterName", ((JdbcNamedParameter) expression).getName());
    } else if (expression instanceof CaseExpression) {
      kind = "case";
    } else if (expression instanceof Select) {
      kind = "subquery";
    } else if (expression instanceof Function || expression instanceof AnalyticExpression
        || expression instanceof CastExpression || expression instanceof ExtractExpression) {
      kind = "function";
      String name = expression instanceof Function ? ((Function) expression).getName()
          : expression instanceof AnalyticExpression ? ((AnalyticExpression) expression).getName()
              : expression instanceof CastExpression ? "cast" : "extract";
      attributes.put("function", name);
      if (AGGREGATES.contains(name.toLowerCase(Locale.ROOT))) {
        attributes.put("aggregate", true);
      }
      if (expression instanceof AnalyticExpression) {
        AnalyticType type = ((AnalyticExpression) expression).getType();
        if (type == AnalyticType.OVER || type == AnalyticType.WITHIN_GROUP_OVER) {
          attributes.put("window", true);
        }
      }
    }
    attributes.put("kind", kind);
    if (expression != null && (!(expression instanceof Column) || column.getDefinition() != null)) {
      attributes.put("expression", expression.toString());
    }
    if (column.getDefinition() != null) {
      attributes.put("definition", column.getDefinition());
    }
    if (column.getRole() != null) {
      attributes.put("role", column.getRole());
    }
    return attributes;
  }
}
