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
import ai.starlake.transpiler.schema.JdbcResultSetMetaData;

import java.sql.SQLException;

import org.json.JSONArray;
import org.json.JSONObject;

public class JsonTreeBuilder extends TreeBuilder<String> {
  public JsonTreeBuilder(JdbcResultSetMetaData resultSetMetaData) {
    super(resultSetMetaData);
  }

  // This builder historically uses a flat child array, unlike JSONObjectTreeBuilder.
  private void flattenChildren(JSONObject node) {
    if (node.has("subquery")) {
      flattenChildren(node.getJSONObject("subquery"));
    }
    JSONArray children = node.optJSONArray("columnSet");
    if (children != null) {
      if (children.length() == 1 && children.optJSONArray(0) != null) {
        children = children.getJSONArray(0);
        node.put("columnSet", children);
      }
      for (int i = 0; i < children.length(); i++) {
        flattenChildren(children.getJSONObject(i));
      }
    }
  }

  @Override
  public String getConvertedTree(JSQLColumResolver resolver) throws SQLException {
    JSONObject tree = new JSONObjectTreeBuilder(resultSetMetaData).getConvertedTree(resolver);
    flattenChildren(tree);
    return tree.toString(2);
  }
}
