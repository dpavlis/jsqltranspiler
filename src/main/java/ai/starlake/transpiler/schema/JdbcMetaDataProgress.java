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

/** Synchronous progress and cancellation for JDBC metadata extraction. */
@FunctionalInterface
public interface JdbcMetaDataProgress {
  enum Phase {
    CATALOGS, SCHEMAS, TABLES, COLUMNS, PRIMARY_KEYS, FOREIGN_KEYS, COMMENTS, COLUMN_DETAILS, INDICES, DONE
  }

  /**
   * Called synchronously on the extracting thread, between JDBC calls (never while a statement or
   * result set of the extraction is open, never inside a savepoint probe).
   *
   * @param phase the extraction phase
   * @param catalog the catalog of the current step, null when the step is not per schema
   * @param schema the schema of the current step, null when the step is not per schema
   * @param table the table of a per-table step, otherwise null
   * @param done units finished in this phase (0 at the phase start)
   * @param total units of this phase, -1 when unknown
   * @throws RuntimeException to abort extraction; propagated to the constructor caller unchanged
   */
  void progress(Phase phase, String catalog, String schema, String table, int done, int total);
}
