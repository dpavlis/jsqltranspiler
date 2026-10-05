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

public class JdbcIndexColumn implements Comparable<JdbcIndexColumn> {

  Short ordinalPosition;
  String columnName;
  String ascOrDesc;
  Long cardinality;
  Long pages;
  String filterCondition;

  public JdbcIndexColumn(Short ordinalPosition, String columnName, String ascOrDesc,
      Long cardinality, Long pages, String filterCondition) {
    this.ordinalPosition = ordinalPosition;
    this.columnName = columnName;
    this.ascOrDesc = ascOrDesc;
    this.cardinality = cardinality;
    this.pages = pages;
    this.filterCondition = filterCondition;
  }

  public Short getOrdinalPosition() {
    return ordinalPosition;
  }

  public String getColumnName() {
    return columnName;
  }

  public String getAscOrDesc() {
    return ascOrDesc;
  }

  public Long getCardinality() {
    return cardinality;
  }

  public Long getPages() {
    return pages;
  }

  public String getFilterCondition() {
    return filterCondition;
  }

  @Override
  public int compareTo(JdbcIndexColumn other) {
    return ordinalPosition.compareTo(other.ordinalPosition);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof JdbcIndexColumn)) {
      return false;
    }
    JdbcIndexColumn that = (JdbcIndexColumn) other;
    return Objects.equals(ordinalPosition, that.ordinalPosition)
        && Objects.equals(columnName, that.columnName) && Objects.equals(ascOrDesc, that.ascOrDesc)
        && Objects.equals(cardinality, that.cardinality) && Objects.equals(pages, that.pages)
        && Objects.equals(filterCondition, that.filterCondition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ordinalPosition, columnName, ascOrDesc, cardinality, pages,
        filterCondition);
  }
}
