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

import ai.starlake.transpiler.JSQLColumResolver;
import ai.starlake.transpiler.schema.treebuilder.JSONObjectTreeBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import java.io.StringReader;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CatalogOnlyLineageTest {
  private JdbcMetaData metadata(String type, boolean json) {
    JdbcMetaData metadata = new JdbcMetaData("test", "",
        new String[][] {{"lt_customers", "name", "id"}, {"lt_orders", "customer_id", "id"}});
    metadata.addTable("other", "", "customers",
        List.of(new JdbcColumn("name"), new JdbcColumn("id")));
    metadata.setDatabaseType(type);
    return json
        ? JdbcJSONSerializer
            .fromJson(new StringReader(JdbcJSONSerializer.toJson(metadata).toString()))
        : metadata;
  }

  private JSONArray lineage(JdbcMetaData metadata, String sql) throws Exception {
    return new JSQLColumResolver(metadata).getLineage(JSONObjectTreeBuilder.class, sql)
        .getJSONArray("columnSet");
  }

  private void assertColumn(JSONObject column, String table, String name) {
    assertEquals(table, column.getString("table"));
    assertEquals(table + "." + name, column.getString("scope"));
    assertFalse(column.toString().contains(".."));
  }

  @ParameterizedTest
  @CsvSource({"MYSQL,false", "MYSQL,true", "MARIADB,false", "MARIADB,true", "OTHER,false",
      "OTHER,true"})
  void catalogQualifiedTablesColumnsAndQuotes(String type, boolean json) throws Exception {
    for (String sql : List.of("select c.name from test.lt_customers c",
        "select c.name from `test`.`lt_customers` c",
        "select test.lt_customers.name from test.lt_customers",
        "select `test`.`lt_customers`.`name` from `test`.`lt_customers`",
        "select c.name from lt_customers c")) {
      assertColumn(lineage(metadata(type, json), sql).getJSONObject(0), "test.lt_customers",
          "name");
    }
    JSONArray joined = lineage(metadata(type, json),
        "select o.id, c.name from lt_orders o " + "join other.customers c on o.customer_id = c.id");
    assertColumn(joined.getJSONObject(0), "test.lt_orders", "id");
    assertColumn(joined.getJSONObject(1), "other.customers", "name");
    JSONArray star = lineage(metadata(type, json), "select c.* from other.customers c");
    assertColumn(star.getJSONObject(0), "other.customers", "name");
  }

  @ParameterizedTest
  @CsvSource({"MYSQL,false", "MYSQL,true", "MARIADB,false", "MARIADB,true"})
  void unknownDatabaseReportsCatalog(String type, boolean json) {
    RuntimeException error = assertThrows(RuntimeException.class,
        () -> lineage(metadata(type, json), "select t.name from nodb.t t"));
    assertTrue(error.getMessage().contains("nodb"));
    assertTrue(error.getMessage().toLowerCase().contains("catalog"), error.getMessage());
  }

  @ParameterizedTest
  @CsvSource({"POSTGRESQL", "MSSQL", "ORACLE", "DB2", "OTHER"})
  void existingSchemaWinsOverCatalogFallback(String type) throws Exception {
    JdbcMetaData metadata = new JdbcMetaData("test", "public");
    metadata.addTable("test", "other", "customers", List.of(new JdbcColumn("name")));
    metadata.addTable("other", "", "customers", List.of(new JdbcColumn("name")));
    metadata.setDatabaseType(type);
    assertColumn(lineage(metadata, "select c.name from other.customers c").getJSONObject(0),
        "test.other.customers", "name");
  }

  @ParameterizedTest
  @CsvSource({"MYSQL,false", "MYSQL,true", "MARIADB,false", "MARIADB,true"})
  void schemaModePreservesDatabaseQualifiers(String type, boolean json) throws Exception {
    JdbcMetaData metadata = new JdbcMetaData("", "test",
        new String[][] {{"lt_customers", "name", "id"}, {"lt_orders", "customer_id", "id"}});
    metadata.addTable("", "other", "customers", List.of(new JdbcColumn("name")));
    metadata.setDatabaseType(type);
    if (json) {
      metadata = JdbcJSONSerializer
          .fromJson(new StringReader(JdbcJSONSerializer.toJson(metadata).toString()));
    }
    for (String sql : List.of("select c.name from test.lt_customers c",
        "select c.name from `test`.`lt_customers` c",
        "select test.lt_customers.name from test.lt_customers",
        "select `test`.`lt_customers`.`name` from `test`.`lt_customers`",
        "select name from lt_customers")) {
      assertColumn(lineage(metadata, sql).getJSONObject(0), "test.lt_customers", "name");
    }
    JSONArray joined = lineage(metadata, "select o.id, c.name from lt_orders o "
        + "join other.customers c on o.customer_id = c.name");
    assertColumn(joined.getJSONObject(0), "test.lt_orders", "id");
    assertColumn(joined.getJSONObject(1), "other.customers", "name");
    JdbcMetaData finalMetadata = metadata;
    RuntimeException error = assertThrows(RuntimeException.class,
        () -> lineage(finalMetadata, "select name from nodb.t"));
    assertTrue(error.getMessage().toLowerCase().contains("schema"));
  }

  @ParameterizedTest
  @CsvSource({"MYSQL", "MARIADB"})
  void namedSchemaWinsEvenInMysqlCatalogMode(String type) throws Exception {
    JdbcMetaData metadata = metadata(type, false);
    metadata.addTable("test", "other", "customers", List.of(new JdbcColumn("name")));
    assertColumn(lineage(metadata, "select name from other.customers").getJSONObject(0),
        "test.other.customers", "name");
  }

}
