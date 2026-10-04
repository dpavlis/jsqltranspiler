# JSQLTranspiler: schema-filtered extraction of JdbcMetaData

## Goal
Extract the metadata of selected schemas only. `new JdbcMetaData(Connection)` scans every catalog, schema, table and column, which is slow and produces huge files on a large warehouse. The behaviour of the existing constructor must not change.

## Current behaviour (the reason for the change)
- **`JdbcMetaData(Connection)`, lines ~417–483:**
  - all catalogs (`JdbcCatalog.getCatalogsFromInformationSchema` / `getCatalogs`);
  - all schemas (`JdbcSchema.getSchemasFromInformationSchema` / `getSchemas`);
  - `JdbcTable.getTables(md, null, null)`;
  - `JdbcTable.getColumns(md)` (`null, "%", "%"`).
- **`JdbcTable.getTablesFromInformationSchema(md, currentCatalog, currentSchema, tableNamePattern)` ignores `currentCatalog` and `currentSchema`.**
  - Its SQL is `SELECT * FROM <conn.getCatalog()>.information_schema.tables WHERE table_name LIKE '<pattern>'`, built with `String.format`, so the pattern is concatenated into the SQL.
  - The schemas are filtered only by `DatabaseSpecific.processSchema`, which skips the system schemas.
  - So there is currently no way to restrict the tables to a schema on the INFORMATION_SCHEMA path.
- **Already fine:** `JdbcTable.getColumnsFromSchemaInformation(conn, catalog, schemaPattern, tableNamePattern)` filters correctly with a PreparedStatement, using `=` or `LIKE` when the pattern contains `%`. The DatabaseMetaData fallbacks of `getTables`/`getColumns` pass the arguments on to JDBC.

## API
```java
/**
 * Derives JDBC MetaData of the schemas matching the patterns.
 *
 * @param conn the physical database connection
 * @param schemaPatterns "schema" or "catalog.schema"; the schema part may contain the SQL LIKE wildcards % and _;
 *                       the catalog part is an exact name (no wildcards). Null or empty = all schemas, identical to
 *                       JdbcMetaData(Connection).
 */
public JdbcMetaData(Connection conn, Collection<String> schemaPatterns) throws SQLException

public JdbcMetaData(Connection conn) throws SQLException {
  this(conn, Collections.emptyList());
}
```
**Helper (public, so callers can validate input):**
- `public static String[] JdbcMetaData.parseSchemaPattern(String pattern)` returns `{catalog, schemaPattern}`. A pattern without a catalog gives `{null, pattern}`.
- The split is at the **last** `catalogSeparator` (`.` by default).
- Surrounding double quotes are stripped from each part (`"My.Cat".public` → `{My.Cat, public}`): the split is done outside quoted parts.
- A blank pattern throws `IllegalArgumentException`.

## Semantics
1. **Current catalog and schema:** read exactly as now, via `databaseType.getCurrentSchemaQuery()`. The pattern does not change them.
2. **Catalogs:**
   - discovered as now;
   - when patterns are given, only the catalogs of the matched schemas are kept, plus the empty catalog `""` (some DBs don't use catalogs).
   - A pattern without a catalog matches schemas in any catalog visible through the connection.
3. **Schemas:**
   - discovered as now;
   - with patterns, a schema is kept when it matches at least one pattern: the catalog matches exactly (case-insensitive) when given, and the schema name matches the LIKE pattern case-insensitively (`%` any sequence, `_` one character, with `\` escaping in the pattern).
   - The empty schema `""` is kept as now.
4. **Tables:**
   - for each pattern, `JdbcTable.getTables(md, catalog, schemaPattern, "%")`;
   - the tables are put only into the catalogs and schemas kept in step 3. A table whose schema isn't kept is ignored, as now.
5. **Columns:**
   - for each pattern, `JdbcTable.getColumns(md, catalog, schemaPattern, "%")`;
   - they are linked to their tables as now (`catalogs.get` → `schema.get` → `table.columns.put`).
6. **Duplicates:** overlapping patterns (`public`, `pub%`) must not create duplicate tables or columns. `put` already replaces by key, so this only needs a test.
7. **`processSchema` (system-schema exclusion):** still applies. An explicit pattern naming an excluded schema (e.g. `INFORMATION_SCHEMA`) still returns nothing, and that is documented.
8. **No match:** the result has no tables, and that is not an error. The caller decides (CloverDX reports zero counts).

## Fix in `JdbcTable.getTablesFromInformationSchema`
- **PreparedStatement.** Use `SELECT * FROM <catalogPrefix>information_schema.tables WHERE 1=1`, then add:
  - `AND TABLE_CATALOG = ?` when the catalog is not null or empty;
  - `AND TABLE_SCHEMA = ?`, or `LIKE ?` when the pattern contains `%` or `_` (the same rule as `getColumnsFromSchemaInformation`, extended to `_`);
  - `AND TABLE_NAME LIKE ?` (default `%`).
- **`<catalogPrefix>`:**
  - the quoted catalog of the pattern (`"<catalog>".`) when one is given; otherwise `conn.getCatalog()` as now. With null, there is no prefix: plain `information_schema.tables`.
  - If querying another catalog's information_schema fails, the existing `SQLException` → DatabaseMetaData fallback applies, and that fallback already passes catalog and schema to `metaData.getTables`.
- **Columns:** apply the same `_` wildcard rule to `getColumnsFromSchemaInformation`, which today switches to `LIKE` only for `%`.
- **Unchanged:** the result columns, the `processSchema` filter and the fallback.

## Not in scope
- Table-name patterns in the constructor. They can be added later as `catalog.schema.table`.
- Wildcards in the catalog part.
- Changes to `JdbcJSONSerializer`. Callers may add top-level keys to the JSON, because `fromJson` reads only known keys; the spec relies on that, so please keep it.

## Compatibility
- `JdbcMetaData(Connection)` produces the same result as before. The only side effect is that `getTablesFromInformationSchema` now really applies the catalog and schema it receives. Its internal callers pass `null, null`, so their results don't change.
- External callers that passed a catalog or schema to `JdbcTable.getTables` will now get filtered results, which is what the parameters always promised. Mention this in the changelog.

## Tests (H2 or HSQLDB in memory, whichever the transpiler tests already use)
Fixture: schemas `SALES` (tables `ORDERS`, `CUSTOMERS`), `STAGE` (table `ORDERS_RAW`) and `STAGE2` (table `X`).
1. `new JdbcMetaData(conn)` returns all three schemas and their tables (regression).
2. `new JdbcMetaData(conn, List.of("SALES"))` returns `SALES` with two tables and their columns, and neither `STAGE` nor `STAGE2`.
3. With `List.of("STAGE%")`: `STAGE` and `STAGE2` are present.
4. With `List.of("STAGE_")`: only `STAGE2`, since `_` matches one character.
5. With `List.of("<currentCatalog>.SALES")`: the same as test 2.
6. With `List.of("SALES", "SAL%")`: no duplicate tables or columns.
7. With `List.of("NOPE")`: no tables, no exception.
8. `parseSchemaPattern`: `"public"` → `{null, public}`; `"dwh.public"` → `{dwh, public}`; `"\"my.db\".public"` → `{my.db, public}`; `""` → IAE.
9. INFORMATION_SCHEMA-path failure (a mocked connection whose information_schema query throws): the filter is still honoured through the DatabaseMetaData fallback.
10. JSON round trip of test 2's result (`toJson` → `fromJson`) gives equal catalogs, schemas, tables and columns. With an extra top-level key (`"format": "x"`) added, `fromJson` still succeeds.

## Delivery
`JSQLTranspiler-1.13-SNAPSHOT.jar`, built for Java 17 against JSqlParser 5.5.108-SNAPSHOT as now. CloverDX copies it to `cloverdx.lineage/lib`.
