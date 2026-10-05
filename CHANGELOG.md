# jsqltranspiler changelog

Changelog of jsqltranspiler

## Unreleased

### Changed

* Snowflake DATE columns use precision 10 when JDBC column metadata reports zero or null,
  matching query result metadata and retaining that value through catalog JSON round trips.

* Schema filters without a catalog now select the connection's current catalog when available.
  Set `JdbcMetaDataOptions.setAllCatalogs(true)` to retain matching across all visible catalogs.
  Empty filter collections still perform unrestricted extraction. Catalog qualifiers are exact
  names, including literal `_` and `%` characters.
* Snowflake reads keys with two schema-wide `SHOW` queries, falling back to schema-wide JDBC
  before per-table calls. Filtered Snowflake/Databricks schema discovery visits only selected
  catalogs and passes schema patterns to JDBC; unreadable automatically discovered catalogs
  are skipped at FINE logging level.

### Added

* Optional synchronous JDBC extraction progress via `JdbcMetaDataOptions.setProgress` or
  `withProgress`. Callbacks report discovery and enrichment phases between JDBC calls;
  throwing a runtime exception cancels extraction and propagates the same exception to the caller.

* Optional Oracle 23 and SQL Server 2022 live catalog regression tests, configured through a
  Git-ignored local properties file. Isolated fixtures exercise enriched metadata, JSON,
  transaction preservation and constant query counts as table counts grow.

* Opt-in catalog enrichment through `JdbcMetaData(Connection, Collection<String>,
  JdbcMetaDataOptions)`: primary keys, imported foreign keys, comments and column details by
  default, with optional approximate indexes. Keys use schema-level bulk strategies and guarded
  JDBC fallbacks; references outside the schema filter are retained. Public key/index getters
  and `JdbcTable.foreignKeys` expose the results.
* Catalog JSON stores ordered primary/foreign keys, referential actions, comments, defaults,
  ordinal positions, identity/generated flags and optional indexes. Readers restore the new
  fields and accept old JSON and unknown keys. Existing connection overloads retain compact JSON
  and do not issue enrichment queries.

* Expression-aware column lineage in JSON and XML: normalized SQL text, chained definitions,
  column/literal/parameter/function/operator/CASE/subquery kinds, literal types and parameter
  identifiers, aggregate/window flags, and condition/value/partition/order roles. Flattened
  dependency output exposes top-level attributes through `getColumnAttributes()`.

* JDBC metadata configurations for SAP HANA (including the `HDB` driver name), Teradata,
  Db2, MariaDB, BigQuery, Amazon Redshift and Databricks (including `SparkSQL`). Each
  configures current catalog/schema lookup and exact system-schema exclusions. New
  configurations accept all JDBC-reported table types to preserve vendor-specific objects.
  Database detection is independent of the JVM locale and accepts null names as `OTHER`.

* Schema-filtered JDBC extraction with `JdbcMetaData(Connection, Collection<String>)` and
  public `parseSchemaPattern(String)`. Schema patterns match case-insensitively with `%`, `_`,
  and backslash escaping; catalogs match exactly, ignoring case. Overlapping patterns query
  each discovered schema once. Unmatched patterns return no tables. Null or empty collections
  retain unrestricted extraction. Database-specific system-schema exclusions still apply.

### Fixed

* Support Oracle and SQL Server transaction scans when savepoint release is unsupported;
  rollback protection remains active. Consume Oracle streaming column defaults before later
  JDBC fields close the stream. Catalog JSON round trips no longer invent a catalog from the
  current database name on catalog-less drivers.

* Preserve unknown column nullability in catalog JSON by omitting `isNullable` instead of writing
  false. Missing/null values read as JDBC unknown. Skip index statistics rows, handle PK-less
  tables, preserve key/index metadata on copies, and compare foreign-key column pairs by content.

* Preserve defining operators and aggregates through subqueries, CTEs and metadata-defined
  views instead of flattening their children and dropping the root operation. Preserve lineage
  when metadata is copied. JSON and XML safely escape expression text. Scalar subqueries
  serialize their original resolved scope, including CTEs and correlated references; copying
  their nodes no longer mutates shared source columns and hides dependencies.

* Share missing catalog/schema defaults across JDBC and INFORMATION_SCHEMA extraction paths,
  using the requested scope or current connection scope when supported by the driver. Drivers
  without catalogs/schemas keep empty qualifiers. Catalog/schema discovery omissions no longer
  prevent tables and columns from attaching. Schema-less drivers receive empty schemas for
  each discovered catalog.
* Protect speculative metadata/current-context queries with savepoints in caller transactions.
  Failed probes roll back only their savepoint before JDBC fallback. Without savepoint support,
  probes are skipped; current context uses connection getters. Savepoint recovery failures
  propagate instead of issuing further queries. Auto-commit and caller work are preserved.

* PostgreSQL metadata extraction now uses JDBC directly, avoiding unsupported INFORMATION_SCHEMA
  probes that abort transactions when auto-commit is disabled. Schema catalogs omitted by the
  PostgreSQL driver are assigned to the connection's current catalog so tables and columns link
  correctly. Null table/column catalogs from older PostgreSQL drivers also fall back to the
  requested catalog or current connection catalog. Extraction preserves the caller's auto-commit setting, pending work and savepoints.

* `JdbcTable.getTables` now honors catalog and schema arguments on the INFORMATION_SCHEMA
  path, including for external callers. Filters use bound parameters and catalog identifiers
  are quoted. Column patterns now recognize `_` as a wildcard alongside `%`.
* H2 now excludes INFORMATION_SCHEMA tables through its shared database policy. This also
  changes unrestricted extraction: the existing constructor no longer includes H2 system tables.

## 1.13

Released 2026-09-18.

### Dependencies

* **jsqlparser 5.4.15** (was 5.4.2 in 1.12). Notable upstream changes: BigQuery
  `JSON 'literal'` string literals parse again (upstreamed from the starlake-ai
  fork, JSQLParser#2488), plus grammar fixes for `MERGE ... WHEN NOT MATCHED BY
  TARGET / BY SOURCE`, `XMLTABLE`, PostgreSQL `GROUPS` window frames, structured
  interval qualifiers and nested parametric `CAST` targets. The full test suite
  (1050 tests) is green against 5.4.15.

### Fixed

* Resolve correlated sub queries against the enclosing query (#152).
* Override the `UnPivotQuery` visit methods in `JSQLColumResolver`: jsqlparser
  5.4.15 adds `UnPivotQuery` with default `visit()` in both `SelectVisitor` and
  `FromItemVisitor`, which broke compilation (same pattern as `PivotQuery` in
  1.12).

## 1.12

Released 2026-09-14.

### Dependencies

* **jsqlparser 5.4.2** (was 5.3.336 in 1.11). The release POM pins the newest
  `com.manticore-projects.jsqlformatter:jsqlparser` release on Maven Central.
  Development builds continue to track the newest manticore snapshot. The full
  test suite (1050 tests) passes against both lines, so no source change was
  required for the upgrade.

### Documentation

* Align `PUBLISHING.md` with the actual `publish.sh` flow.

### Build

* Ignore `.bsp/`, `.claude/` and the `tickitdb.zip` fixture that the test suite
  downloads on demand.
