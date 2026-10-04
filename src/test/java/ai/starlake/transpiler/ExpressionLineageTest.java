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
package ai.starlake.transpiler;

import ai.starlake.transpiler.schema.JdbcMetaData;
import ai.starlake.transpiler.schema.JdbcResultSetMetaData;
import ai.starlake.transpiler.schema.treebuilder.FlattenedColumnBuilder;
import ai.starlake.transpiler.schema.treebuilder.JSONObjectTreeBuilder;
import ai.starlake.transpiler.schema.treebuilder.JsonTreeBuilder;
import ai.starlake.transpiler.schema.treebuilder.XmlTreeBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.DocumentBuilderFactory;
import org.xml.sax.InputSource;

import static org.junit.jupiter.api.Assertions.*;

class ExpressionLineageTest {
  private static final String[][] SCHEMA =
      {{"order_details", "order_id", "quantity", "unit_price", "discount"},
          {"orders", "order_id", "customer_id", "freight", "ship_region", "order_date", "ship_via"},
          {"customers", "customer_id", "company_name"}, {"shippers", "shipper_id"}};

  private JSQLColumResolver resolver() throws Exception {
    return new JSQLColumResolver(new JdbcMetaData(SCHEMA));
  }

  private JSONObject result(String sql) throws Exception {
    return resolver().getLineage(JSONObjectTreeBuilder.class, sql);
  }

  private JSONObject first(String sql) throws Exception {
    return result(sql).getJSONArray("columnSet").getJSONObject(0);
  }

  private List<JSONObject> nodes(JSONObject node) {
    List<JSONObject> all = new ArrayList<>();
    all.add(node);
    JSONArray children = node.optJSONArray("columnSet");
    if (children != null) {
      for (Object child : children) {
        if (child instanceof JSONArray) {
          for (Object nested : (JSONArray) child) {
            all.addAll(nodes((JSONObject) nested));
          }
        } else {
          all.addAll(nodes((JSONObject) child));
        }
      }
    }
    return all;
  }

  private JSONObject named(JSONObject root, String name) {
    return nodes(root).stream().filter(n -> name.equals(n.optString("name"))).findFirst()
        .orElseThrow(() -> new AssertionError("Missing " + name + " in " + root));
  }

  private JSONObject child(JSONObject root) {
    return root.getJSONArray("columnSet").getJSONArray(0).getJSONObject(0);
  }

  @Test
  void arithmeticKeepsExistingNestingAndAddsKinds() throws Exception {
    String expression = "d.quantity * d.unit_price * (1 - d.discount)";
    JSONObject root = first("select " + expression + " as line_total from order_details d");
    assertEquals(expression, root.getString("expression"));
    assertEquals("operator", root.getString("kind"));
    assertEquals("Multiplication", root.getString("name"));
    assertEquals("line_total", root.getString("alias"));
    assertEquals("Multiplication", child(root).getString("name"));
    JSONObject one = named(root, "1");
    assertEquals("literal", one.getString("kind"));
    assertEquals("1", one.getString("value"));
    assertEquals("number", one.getString("literalType"));
    for (String name : List.of("quantity", "unit_price", "discount")) {
      JSONObject leaf = named(root, name);
      assertEquals("column", leaf.getString("kind"));
      assertEquals("order_details", leaf.getString("table"));
      assertEquals("order_details." + name, leaf.getString("scope"));
      assertFalse(leaf.has("expression"));
      assertFalse(leaf.has("columnSet"));
    }
  }

  @Test
  void functionsAndLiterals() throws Exception {
    JSONObject root = result("select upper(c.company_name) as cust, "
        + "coalesce(o.ship_region, 'n/a') as reg from customers c, orders o");
    JSONObject upper = root.getJSONArray("columnSet").getJSONObject(0);
    assertEquals("function", upper.getString("kind"));
    assertEquals("upper", upper.getString("function"));
    assertEquals("upper(c.company_name)", upper.getString("expression"));
    JSONObject coalesce = root.getJSONArray("columnSet").getJSONObject(1);
    assertEquals("coalesce", coalesce.getString("function"));
    JSONObject literal = named(coalesce, "'n/a'");
    assertEquals("literal", literal.getString("kind"));
    assertEquals("string", literal.getString("literalType"));
  }

  @Test
  void casesMarkConditionsAndValuesWithoutChangingOtherReferences() throws Exception {
    JSONObject root = result("select case when d.discount > 0 then 'Y' else 'N' end as disc, "
        + "case d.discount when 0 then d.discount else 1 end as simple, d.discount "
        + "from order_details d");
    JSONObject searched = root.getJSONArray("columnSet").getJSONObject(0);
    assertEquals("case", searched.getString("kind"));
    assertEquals("condition", named(searched, "GreaterThan").getString("role"));
    assertEquals("column", named(searched, "discount").getString("kind"));
    for (String literal : List.of("'Y'", "'N'")) {
      assertEquals("value", named(searched, literal).getString("role"));
    }
    JSONObject simple = root.getJSONArray("columnSet").getJSONObject(1);
    assertEquals("condition", child(simple).getString("role"));
    JSONObject when = named(simple, "WhenClause");
    assertEquals("value", named(when, "discount").getString("role"));
    assertFalse(root.getJSONArray("columnSet").getJSONObject(2).has("role"));
  }

  @Test
  void derivedTablesKeepOperationAndCtesKeepAggregate() throws Exception {
    JSONObject derived =
        first("select x.total from (select o.freight * 2 as total from orders o) x");
    assertEquals("column", derived.getString("kind"));
    assertEquals("x.total", derived.getString("expression"));
    assertEquals("o.freight * 2", derived.getString("definition"));
    assertEquals("Multiplication", child(derived).getString("name"));
    JSONObject cte = first("with t as (select d.order_id, sum(d.quantity * d.unit_price) "
        + "as amount from order_details d group by d.order_id) select t.amount from t");
    assertEquals("t.amount", cte.getString("expression"));
    assertEquals("sum(d.quantity * d.unit_price)", cte.getString("definition"));
    JSONObject sum = child(cte);
    assertEquals("sum", sum.getString("function"));
    assertTrue(sum.getBoolean("aggregate"));
    assertEquals("Multiplication", child(sum).getString("name"));
  }

  @Test
  void definitionsChainAndKnownViewsKeepLineage() throws Exception {
    String sql = "with a as (select o.freight * 2 as total from orders o), "
        + "b as (select a.total as amount from a) select b.amount from b";
    JSONObject root = first(sql);
    assertEquals("a.total", root.getString("definition"));
    assertEquals("o.freight * 2", child(root).getString("definition"));
    assertEquals("Multiplication", child(child(root)).getString("name"));
    JdbcMetaData meta = new JdbcMetaData(SCHEMA);
    JdbcResultSetMetaData definition =
        JSQLColumResolver.getResultSetMetaData("select o.freight * 2 as total from orders o", meta);
    meta.put(definition, "known_view", "view definition");
    root = new JSQLColumResolver(meta)
        .getLineage(JSONObjectTreeBuilder.class, "select v.total from known_view v")
        .getJSONArray("columnSet").getJSONObject(0);
    assertEquals("o.freight * 2", root.getString("definition"));
    assertEquals("Multiplication", child(root).getString("name"));
    assertFalse(first("select o.freight from orders o").has("definition"));
  }

  @ParameterizedTest
  @CsvSource({"1,number", "1.5,number", "NULL,null", "TRUE,boolean", "DATE '1997-01-01',date",
      "TIME '12:34:56',time", "TIMESTAMP '1997-01-01 12:34:56',timestamp",
      "INTERVAL '2' DAY,interval", "INTERVAL (1 + 2) DAY,interval", "0xFF,binary"})
  void literalKinds(String literal, String type) throws Exception {
    JSONObject root = first("select " + literal + " as x");
    assertEquals("literal", root.getString("kind"));
    assertEquals(type, root.getString("literalType"));
    assertEquals(root.getString("expression"), root.getString("value"));
  }

  @Test
  void parametersKeepStatementOrderAndNames() throws Exception {
    JSONObject root =
        result("select ? as a, coalesce(?, ?) as b from orders o " + "where o.customer_id = ?");
    assertEquals(1, root.getJSONArray("columnSet").getJSONObject(0).getInt("index"));
    List<JSONObject> params =
        nodes(root).stream().filter(n -> "parameter".equals(n.optString("kind"))).toList();
    assertEquals(List.of(1, 2, 3), params.stream().map(n -> n.getInt("index")).toList());
    assertEquals("p", first("select :p as x").getString("parameterName"));
  }

  @Test
  void windowAndPredicateFunctionRoles() throws Exception {
    JSONObject root = first("select sum(o.freight) over (partition by o.customer_id "
        + "order by o.order_date) as running from orders o");
    assertTrue(root.getBoolean("window"));
    assertTrue(root.getBoolean("aggregate"));
    assertEquals("partition", named(root, "customer_id").getString("role"));
    assertEquals("order", named(root, "order_date").getString("role"));
    root = first("select sum(o.freight) filter (where o.freight > 0) from orders o");
    assertFalse(root.has("window"));
    assertEquals("condition", named(root, "GreaterThan").getString("role"));
    for (String function : List.of("nullif(o.freight, o.customer_id)",
        "iif(o.customer_id > 0, o.freight, 0)", "if(o.customer_id > 0, o.freight, 0)")) {
      root = first("select " + function + " from orders o");
      String name = function.startsWith("nullif") ? "customer_id" : "GreaterThan";
      assertEquals("condition", named(root, name).getString("role"));
    }
  }

  @Test
  void castsExtractsAndNonconstantIntervalsKeepTheirOperands() throws Exception {
    JSONObject cast = first("select cast(o.freight as decimal(10,2)) as f from orders o");
    assertEquals("function", cast.getString("kind"));
    assertEquals("cast", cast.getString("function"));
    assertEquals("freight", child(cast).getString("name"));
    JSONObject extract = first("select extract(year from o.order_date) from orders o");
    assertEquals("function", extract.getString("kind"));
    assertEquals("extract", extract.getString("function"));
    assertEquals("order_date", child(extract).getString("name"));
    JSONObject interval = first("select interval (d.quantity) day from order_details d");
    assertEquals("operator", interval.getString("kind"));
    assertEquals("column", named(interval, "quantity").getString("kind"));
    JSONObject repeated = first("select nullif(o.freight, o.freight) from orders o");
    JSONArray args = repeated.getJSONArray("columnSet").getJSONArray(0);
    assertFalse(args.getJSONObject(0).has("role"));
    assertEquals("condition", args.getJSONObject(1).getString("role"));
  }

  @Test
  void starColumnsStayPlainAndScalarSubqueriesHaveKind() throws Exception {
    JSONObject root =
        result("select * from shippers s, orders o " + "where o.ship_via = s.shipper_id");
    for (Object item : root.getJSONArray("columnSet")) {
      JSONObject column = (JSONObject) item;
      assertEquals("column", column.getString("kind"));
      assertFalse(column.has("expression"));
    }
    root = first("select (select max(o.freight) from orders o) as m");
    assertEquals("subquery", root.getString("kind"));
    assertTrue(root.has("subquery"));
  }

  @Test
  void scalarSubqueriesRetainCteAndCorrelatedScopes() throws Exception {
    JSONObject root = first("with t as (select o.freight * 2 as total from orders o) "
        + "select (select t.total from t) as x");
    JSONObject inner = root.getJSONObject("subquery").getJSONArray("columnSet").getJSONObject(0);
    assertEquals("o.freight * 2", inner.getString("definition"));
    root = first("select (select o.freight + 1 from customers c) as x from orders o");
    inner = root.getJSONObject("subquery").getJSONArray("columnSet").getJSONObject(0);
    assertEquals("o.freight + 1", inner.getString("expression"));
    assertEquals("orders", named(inner, "freight").getString("table"));
  }

  @Test
  void buildersExposeAttributesAndEscapeSqlText() throws Exception {
    String sql = "select coalesce(o.ship_region, 'a<\"&b') as reg from orders o";
    JSQLColumResolver resolver = resolver();
    JSONObject json = new JSONObject(resolver.getLineage(JsonTreeBuilder.class, sql));
    JSONObject root = json.getJSONArray("columnSet").getJSONObject(0);
    assertEquals("function", root.getString("kind"));
    assertEquals(2, root.getJSONArray("columnSet").length());
    String xml = resolver.getLineage(XmlTreeBuilder.class, sql);
    org.w3c.dom.Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder()
        .parse(new InputSource(new StringReader(xml)));
    org.w3c.dom.Element column = (org.w3c.dom.Element) doc.getElementsByTagName("Column").item(0);
    assertEquals("function", column.getAttribute("kind"));
    assertEquals(root.getString("expression"), column.getAttribute("expression"));
    FlattenedColumnBuilder flat = new FlattenedColumnBuilder(resolver.getResultSetMetaData(sql));
    assertEquals(java.util.Set.of("orders.ship_region"),
        flat.getConvertedTree(resolver).get("reg"));
    assertEquals(root.getString("expression"),
        flat.getColumnAttributes().get("reg").get("expression"));
    assertEquals(root.getString("kind"), flat.getColumnAttributes().get("reg").get("kind"));
  }
}
