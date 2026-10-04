# jsqltranspiler changelog

Changelog of jsqltranspiler

## Unreleased

### Added

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
