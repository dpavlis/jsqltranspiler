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

/** Opt-in metadata enrichment. Existing connection constructors use {@link #none()}. */
public final class JdbcMetaDataOptions {
  boolean primaryKeys = true;
  boolean foreignKeys = true;
  boolean comments = true;
  boolean columnDetails = true;
  boolean indices;
  boolean allCatalogs;

  public static JdbcMetaDataOptions defaults() {
    return new JdbcMetaDataOptions();
  }

  public static JdbcMetaDataOptions none() {
    return new JdbcMetaDataOptions().setPrimaryKeys(false).setForeignKeys(false).setComments(false)
        .setColumnDetails(false).setIndices(false);
  }

  JdbcMetaDataOptions copy() {
    return new JdbcMetaDataOptions().setPrimaryKeys(primaryKeys).setForeignKeys(foreignKeys)
        .setComments(comments).setColumnDetails(columnDetails).setIndices(indices)
        .setAllCatalogs(allCatalogs);
  }

  /** Allow catalog-less schema filters to match every visible catalog. */
  public JdbcMetaDataOptions setAllCatalogs(boolean enabled) {
    allCatalogs = enabled;
    return this;
  }

  public boolean isAllCatalogs() {
    return allCatalogs;
  }

  public boolean isPrimaryKeys() {
    return primaryKeys;
  }

  public JdbcMetaDataOptions setPrimaryKeys(boolean enabled) {
    primaryKeys = enabled;
    return this;
  }

  public JdbcMetaDataOptions withPrimaryKeys(boolean enabled) {
    return setPrimaryKeys(enabled);
  }

  public boolean isForeignKeys() {
    return foreignKeys;
  }

  public JdbcMetaDataOptions setForeignKeys(boolean enabled) {
    foreignKeys = enabled;
    return this;
  }

  public JdbcMetaDataOptions withForeignKeys(boolean enabled) {
    return setForeignKeys(enabled);
  }

  public boolean isComments() {
    return comments;
  }

  public JdbcMetaDataOptions setComments(boolean enabled) {
    comments = enabled;
    return this;
  }

  public JdbcMetaDataOptions withComments(boolean enabled) {
    return setComments(enabled);
  }

  public boolean isColumnDetails() {
    return columnDetails;
  }

  public JdbcMetaDataOptions setColumnDetails(boolean enabled) {
    columnDetails = enabled;
    return this;
  }

  public JdbcMetaDataOptions withColumnDetails(boolean enabled) {
    return setColumnDetails(enabled);
  }

  public boolean isIndices() {
    return indices;
  }

  public JdbcMetaDataOptions setIndices(boolean enabled) {
    indices = enabled;
    return this;
  }

  public JdbcMetaDataOptions withIndices(boolean enabled) {
    return setIndices(enabled);
  }
}
