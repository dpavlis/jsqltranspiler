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

import ai.starlake.transpiler.schema.JdbcColumn;
import ai.starlake.transpiler.schema.JdbcMetaData;
import ai.starlake.transpiler.schema.treebuilder.JSONObjectTreeBuilder;
import ai.starlake.transpiler.schema.treebuilder.XmlTreeBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.DocumentBuilderFactory;

import static org.junit.jupiter.api.Assertions.*;

/** Regressions for column resolution, pipe transpilation and lineage serialization. */
class ResolverRegressionTest {
  private static final String[][] SCHEMA = {{"t", "a", "b", "c"}};

  private JSONArray columns(JdbcMetaData metaData, String sql) throws Exception {
    return new JSQLColumResolver(metaData).getLineage(JSONObjectTreeBuilder.class, sql)
        .getJSONArray("columnSet");
  }

  private List<String> names(JSONArray columns) {
    List<String> names = new ArrayList<>();
    for (int i = 0; i < columns.length(); i++) {
      names.add(columns.getJSONObject(i).getString("name").toLowerCase());
    }
    return names;
  }

  @ParameterizedTest
  @ValueSource(strings = {"SELECT * EXCLUDE (b) FROM t", "SELECT t.* EXCLUDE (b) FROM t",
      "SELECT * EXCEPT (b) FROM t", "SELECT t.* EXCEPT (b) FROM t"})
  void excludeAndExceptRemoveColumnsFromStar(String sql) throws Exception {
    assertEquals(List.of("a", "c"), names(columns(new JdbcMetaData(SCHEMA), sql)));
  }

  @ParameterizedTest
  @ValueSource(strings = {"SELECT CASE WHEN a = 1 THEN STRUCT(1 AS x) END AS s FROM t",
      "SELECT CASE WHEN a = 1 THEN 1 ELSE STRUCT(a AS x) END AS s FROM t",
      "SELECT CASE STRUCT(a AS x) WHEN b THEN 1 END AS s FROM t",
      "SELECT IF(a = 1, STRUCT(1 AS x), NULL) AS s FROM t",
      "SELECT NULLIF(STRUCT(1 AS x), b) AS s FROM t"})
  void expressionsWithoutColumnsDoNotBreakLineage(String sql) throws Exception {
    JSONArray result = columns(new JdbcMetaData(SCHEMA), sql);
    assertEquals(1, result.length());
    assertEquals("s", result.getJSONObject(0).getString("alias"));
  }

  @Test
  void catalogOnlyTableKeepsEmptySchemaInStrictMode() throws Exception {
    JdbcMetaData metaData =
        new JdbcMetaData("cat1", "main").addTable("cat1", "main", "t", List.of(new JdbcColumn("a")))
            .addTable("db2", "", "x", List.of(new JdbcColumn("q"), new JdbcColumn("a")));
    metaData.setErrorMode(JdbcMetaData.ErrorMode.STRICT);

    JSONObject q = columns(metaData, "SELECT q FROM db2.x").getJSONObject(0);
    assertEquals("db2.x", q.getString("table"));
    assertEquals(List.of("q", "a"), names(columns(metaData, "SELECT * FROM db2.x")));
    JSONArray joined = columns(metaData, "SELECT x.q, t.a FROM db2.x JOIN t ON x.a = t.a");
    assertEquals("db2.x", joined.getJSONObject(0).getString("table"));
    assertEquals("cat1.main.t", joined.getJSONObject(1).getString("table"));
    assertEquals(List.of("q", "a"),
        names(columns(metaData, "SELECT * FROM db2.x NATURAL JOIN db2.x y")).subList(0, 2));
  }

  @ParameterizedTest
  @ValueSource(strings = {"FROM t |> SET b = 10 |> DROP a", "FROM t |> DROP a |> SET b = 10"})
  void pipeSetAndDropTranspileToValidDuckDb(String pipe) throws Exception {
    String sql = JSQLTranspiler.transpileQuery(pipe, JSQLTranspiler.Dialect.GOOGLE_BIG_QUERY);
    String upper = sql.toUpperCase();
    assertTrue(upper.indexOf("EXCLUDE") < upper.indexOf("REPLACE"), sql);
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        Statement st = conn.createStatement()) {
      st.execute("CREATE TABLE t(a INT, b INT, c INT)");
      st.execute("INSERT INTO t VALUES (1, 2, 3)");
      try (ResultSet rs = st.executeQuery(sql)) {
        assertEquals(2, rs.getMetaData().getColumnCount(), sql);
        assertTrue(rs.next());
        assertEquals(10, rs.getInt("b"));
        assertEquals(3, rs.getInt("c"));
      }
    }
  }

  @Test
  void xmlEscapesTableScopeAndTypeAttributes() throws Exception {
    JdbcMetaData metaData = new JdbcMetaData("c<1>", "s&'2");
    JdbcColumn column = new JdbcColumn("a");
    column.typeName = "ENUM('x','y')";
    metaData.addTable("c<1>", "s&'2", "o'k", List.of(column));
    String xml =
        new JSQLColumResolver(metaData).getLineage(XmlTreeBuilder.class, "SELECT a FROM \"o'k\"");
    NodeList nodes = DocumentBuilderFactory.newInstance().newDocumentBuilder()
        .parse(new InputSource(new StringReader(xml))).getElementsByTagName("Column");
    Element a = (Element) nodes.item(0);
    assertEquals("c<1>.s&'2.o'k", a.getAttribute("table"), xml);
    assertEquals("c<1>.s&'2.o'k.a", a.getAttribute("scope"), xml);
    assertEquals("ENUM('x','y')", a.getAttribute("typeName"), xml);
  }
}
