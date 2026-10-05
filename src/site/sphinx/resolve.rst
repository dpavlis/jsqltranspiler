.. meta::
   :description: Java Software Library for rewriting Big RDBMS Queries into Duck DB compatible queries.
   :keywords: java sql query transpiler DuckDB H2 BigQuery Snowflake Redshift DataBricks

*****************
Resolve Columns
*****************

JSQLTranspiler can resolve the STAR operator ``*`` for a given SQL statement and schema without executing the query against a database.
It will return either a JDBC compliant ``ResultSetMetaData`` holding the Column information or rewrite the SQL statement replacing the Star Operator with the actual column names.

Step 1: Providing Schema information
************************************

Schema information can be derive from a JDBC Database Connection or from a virtual JDBC DatabaseMetaData Object.
See the Java API for all the available constructors and methods.

.. code-block:: java
    :caption: Schema Information
    :substitutions:

    // Derive schema from an existing physical database
    Connection conn = ...
    JdbcMetaData metaData = new JdbcMetaData(conn);

    // Or create schema information for a given catalog and schema
    // adding two tables
    JdbcMetaData metaData = new JdbcMetaData(catalogName, schemaName)
        .addTable("a", new JdbcColumn("col1"), new JdbcColumn("col2"), new JdbcColumn("col3"), new JdbcColumn("colAA"), new JdbcColumn("colAB"))
        .addTable("b", new JdbcColumn("col1"), new JdbcColumn("col2"), new JdbcColumn("col3"), new JdbcColumn("colBA"), new JdbcColumn("colBB"));

    // Simplified for an empty catalog and empty schema
    JdbcMetaData metaData = new JdbcMetaData()
                                .addTable("a", "col1", "col2", "col3", "colAA", "colAB")
                                .addTable("b","col1", "col2", "col3", "colBA", "colBB");

    // Further Simplified for an empty catalog and empty schema
    String[][] schemaDefinition = {
        // table a with columns
        {"a", "col1", "col2", "col3, "colAA", "colAB"}

        // table b with columns
        , {"b", "col1", "col2", "col3", "colBA", "colBB"}
    };
    JdbcMetaData metaData = new JdbcMetaData(schemaDefinition);


To restrict extraction to selected schemas, pass schema patterns. Catalog and schema discovery
still runs, but only matched schemas have their tables and columns extracted:

.. code-block:: java

    JdbcMetaData selected = new JdbcMetaData(conn, List.of("SALES", "STAGE%"));
    String[] parts = JdbcMetaData.parseSchemaPattern("\"my.db\".public");
    // parts: {"my.db", "public"}

Patterns use ``schema`` or ``catalog.schema``. Catalog names match exactly, ignoring case;
schema patterns match case-insensitively with ``%`` for any sequence, ``_`` for one character,
and backslash to escape the following character. Double quotes protect dots within names,
and doubled double quotes represent a literal quote. In catalog names, ``_`` and ``%``
are ordinary literal characters; ``SNOWFLAKE_SAMPLE_DATA.TPCH_SF1`` needs no quoting.
Catalog-less filters select the current catalog when one is available. To match such
filters across every visible catalog, use ``JdbcMetaDataOptions.none().setAllCatalogs(true)``
(or ``defaults().setAllCatalogs(true)`` when requesting enrichment).
Matching uses discovered names, so lowercase patterns also retrieve uppercase schemas.
Overlapping patterns extract each matched schema once. The current catalog and schema are
unchanged. Null or empty pattern collections retain unrestricted extraction; unmatched patterns
produce no tables. Empty catalog/schema placeholders remain available.

Database-specific system-schema exclusions apply even to explicit patterns. H2 excludes
``INFORMATION_SCHEMA`` tables for both filtered and unrestricted extraction. JSON serialization
is unchanged, including acceptance of additional top-level keys when reading metadata.


Database-specific metadata configurations
========================================

``JdbcUtils.DatabaseSpecific`` recognizes Oracle, PostgreSQL, Microsoft SQL Server,
MySQL, Snowflake, DuckDB, H2, and the following additional products:

.. list-table:: Additional JDBC database configurations
    :header-rows: 1
    :widths: 20 30 50

    * - Database
      - Product-name matching
      - Current catalog / schema lookup
    * - SAP HANA
      - HANA or HDB
      - DATABASE_NAME and CURRENT_SCHEMA from SYS.M_DATABASE
    * - Teradata
      - TERADATA
      - Empty catalog and DATABASE (Teradata databases are JDBC schemas)
    * - Db2
      - DB2, including platform-qualified names
      - CURRENT SERVER and CURRENT SCHEMA from SYSIBM.SYSDUMMY1
    * - MariaDB
      - MARIADB, before MYSQL matching
      - DATABASE() for both values, following the existing MySQL convention
    * - BigQuery
      - BIGQUERY or BIG QUERY
      - Default dataset project (or execution project) and default dataset
    * - Amazon Redshift
      - REDSHIFT, before POSTGRESQL matching
      - current_database() and current_schema()
    * - Databricks
      - DATABRICKS, SPARKSQL or SPARK SQL
      - current_catalog() and current_schema()

New configurations request all JDBC-reported table types, preserving external tables,
materialized objects, aliases, and other vendor-specific types. Exclusions match exact
schema names, ignoring case. HANA excludes SYS, SYS_DATABASES, _SYS_STATISTICS and
_SYS_REPO, while keeping _SYS_BIC application views available. Teradata excludes DBC
and Sys_Calendar. Db2 excludes its standard catalog, administrative, routine and package
schemas. MariaDB excludes information_schema, mysql, performance_schema and sys.
BigQuery and Databricks exclude INFORMATION_SCHEMA. Redshift excludes INFORMATION_SCHEMA,
pg_catalog, pg_internal, pg_toast, pg_automv and catalog_history. Existing database schema-exclusion policies
are unchanged. PostgreSQL uses JDBC metadata directly for catalogs, schemas, tables and columns,
including calls to the public INFORMATION_SCHEMA helpers. Its driver may omit the catalog on
schema, table and column rows (including pgJDBC 42.7.3). Missing table/column catalogs use
the requested catalog, or the connection's current catalog when none was requested;
schema catalogs use the connection's current catalog. Extraction never
changes auto-commit or commits/rolls back caller transactions.

Snowflake and Databricks use JDBC discovery for catalogs, schemas, tables and columns.
Unrestricted schema discovery enumerates visible catalogs and the current catalog.
Filtered discovery calls ``getSchemas(catalog, schemaPattern)`` only for the catalogs
selected by the filters. Unreadable visible catalogs are skipped with a FINE log;
unknown requested catalogs and ordinary JDBC failures still report errors. Unsupported catalog-scoped schema reads fall back
to one unscoped read; rows lacking a catalog require an unambiguous catalog context.
Tables and columns are read per selected schema, including when no filter is supplied.
Vendor key and comment enrichment remains available. Explicit Snowflake column reads
through ``getColumnsFromSchemaInformation`` use the requested database's quoted
INFORMATION_SCHEMA qualifier, defaulting to the connection's current database.

These configurations supply detection and extraction policy, not dedicated vendor metadata
implementations. INFORMATION_SCHEMA queries retain their existing JDBC fallbacks. Actual
catalog/schema mapping and metadata availability depend on the driver and permissions;
for example, MariaDB drivers may expose databases as catalogs instead of schemas.
Databricks system-catalog telemetry schemas are not globally excluded by the schema-only policy.
A BigQuery connection without a default dataset may report an empty current schema.

Vendor references: `HANA database name
<https://help.sap.com/docs/SLTOOLSET/d4ad61b0bcc143b19ff737ef0796fd9b/fa3f4554f82b1d5de10000000a44538d.html>`_,
`Teradata JDBC metadata
<https://teradata-docs.s3.amazonaws.com/doc/connectivity/jdbc/reference/current/jdbcug_chapter_3.html>`_,
`Db2 special registers
<https://www.ibm.com/docs/en/db2-for-zos/12.0.0?topic=statements-set-schema>`_,
`MariaDB DATABASE()
<https://mariadb.com/docs/server/reference/sql-functions/secondary-functions/information-functions/database>`_,
`BigQuery system variables
<https://docs.cloud.google.com/bigquery/docs/reference/system-variables>`_,
`Redshift system information functions
<https://docs.aws.amazon.com/redshift/latest/dg/r_System_information_functions.html>`_,
and `Databricks catalog/schema lookup
<https://docs.databricks.com/aws/en/query>`_.


Metadata fallback behavior
==========================

Both JDBC and INFORMATION_SCHEMA result mappings use the same defaults for missing catalog
and schema values. An explicitly requested catalog or literal schema takes precedence; otherwise
connection getters provide the current scope when JDBC advertises catalog/schema support.
Drivers that genuinely have no catalogs or schemas retain empty qualifiers. Schema-less drivers
receive an empty schema for every discovered catalog. Catalog discovery omissions are repaired
from the current connection catalog or schema rows, preventing orphaned metadata.

In a caller transaction, speculative SQL (including current-context lookup) runs inside a
savepoint. On query failure the scanner rolls back only to that savepoint before JDBC fallback.
It never commits, rolls back the whole transaction, or changes auto-commit. Drivers without
savepoint support skip SQL probes and use JDBC metadata. Safely recovered current-context
query failures also fall back to independent connection catalog/schema getters.
Failures while creating or rolling back a savepoint, and actual release failures, are propagated
so the scanner cannot continue using an uncertain transaction. Unsupported release is harmless:
Oracle and SQL Server savepoints remain until the caller ends the transaction. With auto-commit enabled, normal query fallback
continues unchanged.

All database profiles have simulated-driver regression coverage for missing catalog/schema
values, failed probes and absent savepoint support. PostgreSQL additionally has live coverage
against PostgreSQL 12.10 using drivers 42.7.3 and 42.7.13. Oracle 23 and SQL Server 2022 have
optional live coverage for catalog enrichment and transaction preservation. Other products
require live integration checks with their own drivers and server versions.


Step 2: Rewrite the Star Operators
************************************

.. code-block:: sql
    :caption: Sample Input Query
    :substitutions:

    SELECT *
    FROM (  (   SELECT *
                FROM b ) c
                INNER JOIN a
                    ON c.col1 = a.col1 ) d
    ;


.. code-block:: java
    :caption: Rewriting Star Operators
    :substitutions:

    String[][] schemaDefinition = {

        // table a with columns
        {"a", "col1", "col2", "col3, "colAA", "colAB"}

        // table b with columns
        , {"b", "col1", "col2", "col3", "colBA", "colBB"}
    };

    String sqlStr = "SELECT * FROM ( (SELECT * FROM b) c inner join a on c.col1 = a.col1 ) d ";

    // get the List of JdbcColumns, each holding its lineage using the TreeNode interface
    JSQLColumResolver resolver = new JSQLColumResolver(schemaDefinition);
    String actual = resolver.getResolvedStatementText(sqlStr);


.. code-block:: sql
    :caption: Rewritten Output Query
    :substitutions:

    SELECT  d.col1
            , d.col2
            , d.col3
            , d.colBA
            , d.colBB
            , d.col1_1
            , d.col2_1
            , d.col3_1
            , d.colAA
            , d.colAB
    FROM (  (   SELECT  b.col1
                        , b.col2
                        , b.col3
                        , b.colBA
                        , b.colBB
                FROM b ) c
                INNER JOIN a
                    ON c.col1 = a.col1 ) d
    ;


Step 3: Resolve Query against Schema
************************************

.. code-block:: sql
    :caption: Sample Query
    :substitutions:

    SELECT  Sum( colBA + colBB ) AS total
            , ( SELECT col1 AS test
                FROM b ) col2
            , CURRENT_TIMESTAMP() AS col3
    FROM a
        INNER JOIN (    SELECT *
                        FROM b ) c
            ON a.col1 = c.col1
    ;


.. code-block:: java
    :caption: Column Resolution
    :substitutions:

    String sqlStr =
        "SELECT Sum(colBA + colBB) AS total, (SELECT col1 AS test FROM b) col2, CURRENT_TIMESTAMP() as col3 FROM a INNER JOIN (SELECT * FROM b) c ON a.col1 = c.col1";

    // get the List of JdbcColumns, each holding its lineage using the TreeNode interface
    JSQLColumResolver resolver = new JSQLColumResolver(schemaDefinition);
    JdbcResultSetMetaData resultSetMetaData = resolver.getResultSetMetaData(sqlStr);

    // loop through the columns at will using the regular ResultSetMetaData semantics
    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
      resultSetMetaData.getColumnName(i);
      resultSetMetaData.getTableName(i);
      resultSetMetaData.getColumnLabel(i);
    }


Step 4: Access the Lineage
************************************

The returned ``ResultSetMetaData`` hold a list of ``JdbcColums`` which implement the ``TreeNode`` interface. It can be used to translate the Lineage into any Tree-like structure by providing a specific ``TreeBuilder``.
There are TreeBuilder Templates for Ascii Trees, JSON Text and XML Text included which you can use to derive your own ``TreeBuilder`` implementation easily.

.. tab:: XML

    .. code-block:: java
        :caption: Lineage XML output
        :substitutions:

        String sqlStr =
            "SELECT Sum(colBA + colBB) AS total, (SELECT col1 AS test FROM b) col2, CURRENT_TIMESTAMP() as col3 FROM a INNER JOIN (SELECT * FROM b) c ON a.col1 = c.col1";

        // get the List of JdbcColumns, each holding its lineage using the TreeNode interface
        JSQLColumResolver resolver = new JSQLColumResolver(schemaDefinition);
        JdbcResultSetMetaData resultSetMetaData = resolver.getResultSetMetaData(sqlStr);

        // get XML text representation of the lineage
        String s = resolver.getLineage(XmlTreeBuilder.class, sqlStr);


    .. code-block:: xml

        <?xml version="1.0" encoding="UTF-8"?>
        <ColumnSet>
            <Column alias='total' name='Sum'>
                <ColumnSet>
                    <Column name='Addition'>
                        <ColumnSet>
                            <Column name='colBA' table='c' scope='b.colBA' dataType='java.sql.Types.OTHER' typeName='Other' columnSize='0' decimalDigits='0' nullable=''/>
                            <Column name='colBB' table='c' scope='b.colBB' dataType='java.sql.Types.OTHER' typeName='Other' columnSize='0' decimalDigits='0' nullable=''/>
                        </ColumnSet>
                    </Column>
                </ColumnSet>
            </Column>
            <Column alias='col2' name='col1'>
                <ColumnSet>
                    <Column alias='test' name='col1' table='b' dataType='java.sql.Types.OTHER' typeName='Other' columnSize='0' decimalDigits='0' nullable=''/>
                </ColumnSet>
            </Column>
            <Column alias='col3' name='CURRENT_TIMESTAMP'/>
        </ColumnSet>




.. tab:: JSON

    .. code-block:: java
        :caption: Lineage JSON output
        :substitutions:

        String sqlStr =
            "SELECT Sum(colBA + colBB) AS total, (SELECT col1 AS test FROM b) col2, CURRENT_TIMESTAMP() as col3 FROM a INNER JOIN (SELECT * FROM b) c ON a.col1 = c.col1";

        // get the List of JdbcColumns, each holding its lineage using the TreeNode interface
        JSQLColumResolver resolver = new JSQLColumResolver(schemaDefinition);
        JdbcResultSetMetaData resultSetMetaData = resolver.getResultSetMetaData(sqlStr);

        // get JSON text representation of the lineage
        String s = resolver.getLineage(JsonTreeBuilder.class, sqlStr);


    .. code-block:: json

        {
            "columnSet": [
                {
                "name": "Sum",
                "alias": "total",
                "columnSet": [
                    {
                        "name": "Addition",
                        "columnSet": [
                            {
                                "name": "colBA"
                                "table": "c",
                                "scope": "b.colBA",
                                "dataType": "java.sql.Types.OTHER",
                                "typeName": "Other",
                                "columnSize": 0,
                                "decimalDigits": 0,
                                "nullable":
                            },
                            {
                                "name": "colBB"
                                "table": "c",
                                "scope": "b.colBB",
                                "dataType": "java.sql.Types.OTHER",
                                "typeName": "Other",
                                "columnSize": 0,
                                "decimalDigits": 0,
                                "nullable":
                            }
                        ]
                    }
                ]
                },
                {
                    "name": "col1",
                    "alias": "col2",
                    "subquery": {
                        "columnSet": [
                            {
                                "name": "col1",
                                "alias": "test"
                                "table": "b",
                                "dataType": "java.sql.Types.OTHER",
                                "typeName": "Other",
                                "columnSize": 0,
                                "decimalDigits": 0,
                                "nullable":
                            }
                        ]
                    }
                },
                {
                    "name": "CURRENT_TIMESTAMP",
                    "alias": "col3"
                }
            ]
        }


.. tab:: ASCII Tree-like

    .. code-block:: java
        :caption: Lineage ASCII Tree output
        :substitutions:

        String sqlStr =
            "SELECT Sum(colBA + colBB) AS total, (SELECT col1 AS test FROM b) col2, CURRENT_TIMESTAMP() as col3 FROM a INNER JOIN (SELECT * FROM b) c ON a.col1 = c.col1";

        // get the List of JdbcColumns, each holding its lineage using the TreeNode interface
        JSQLColumResolver resolver = new JSQLColumResolver(schemaDefinition);
        JdbcResultSetMetaData resultSetMetaData = resolver.getResultSetMetaData(sqlStr);

        // get JSON text representation of the lineage
        String s = resolver.getLineage(AsciiTreeBuilder.class, sqlStr)


    .. code-block:: text

        SELECT
        ├─total AS Function Sum
        │  └─Addition: colBA + colBB
        │     ├─c.colBA → b.colBA : Other
        │     └─c.colBB → b.colBB : Other
        ├─col2 AS SELECT
        │  └─test AS b.col1 : Other
        └─col3 AS TimeKeyExpression: CURRENT_TIMESTAMP()



PostgreSQL transaction regression tests
=======================================

The optional live tests use a database containing at least one user table in its current schema.
Set ``postgresql.url``, ``postgresql.user`` and ``postgresql.password`` in the local,
Git-ignored ``live-databases.properties`` file, then run ``./gradlew test --tests '*PostgreSqlMetaDataTest'``. Credentials are not stored in the
repository. Tests cover filtered/unfiltered extraction with auto-commit on and off. Transactional
tests create a temporary marker table and roll it back, checking that scanning neither commits
nor discards earlier work and that caller savepoints remain valid.

Use ``-PpostgresJdbcVersion=42.7.3`` to run these tests with an older JDBC driver; the default
is 42.7.13. Live test output records the server/driver versions and extracted table/column counts.

Oracle and SQL Server live catalog tests
=======================================

Copy ``live-databases.properties.example`` to ``live-databases.properties`` and fill in the
``oracle.url``, ``oracle.user``, ``oracle.password`` and/or corresponding ``mssql.*`` properties.
The local file is ignored by Git; keep it outside source/resources directories. An alternative
private file can be selected with ``-PliveDatabasesFile=/absolute/path/to/private.properties``.
Absent files or unconfigured endpoints skip the corresponding tests; configured connection
failures fail the tests. Run::

    ./gradlew test --tests '*OracleSqlServerMetaDataTest'

The tests create uniquely named tables in Oracle's existing login schema and a uniquely named
SQL Server schema. They explicitly drop their fixtures afterward because Oracle DDL commits.
The accounts need permissions to create/drop these fixtures, add comments and read catalog
metadata. Tests preserve existing objects and verify primary/composite foreign keys, cascade
rules, comments, defaults, identity/generated flags, ordinal positions, optional indexes,
case-insensitive schema filtering, JSON round trips, and empty unmatched results. Caller work,
connection scope and savepoints are checked with auto-commit both enabled and disabled. Metadata
call counts must remain constant when 20 tables are added; per-table key fallbacks fail the test.

Default test drivers are Oracle ojdbc11 23.6.0.24.10 and Microsoft JDBC 12.8.1.jre11; override with
``-PoracleJdbcVersion=...`` and ``-PmssqlJdbcVersion=...``. Drivers are test-runtime dependencies
and are not bundled into the transpiler JAR.

Snowflake sample database live tests
====================================

Configure ``snowflake.url``, ``snowflake.user`` and ``snowflake.password`` in the ignored
``live-databases.properties`` file, using ``SNOWFLAKE_SAMPLE_DATA`` and ``TPCH_SF1`` as
the connection's database and schema. Run::

    ./gradlew test --tests '*SnowflakeLiveMetaDataTest'

These tests only read the shared sample database. They verify native metadata, numeric
precision, JSON round trips, join lineage, discovery from another current database, and
explicit INFORMATION_SCHEMA reads. An existing warehouse is selected for SQL queries;
if none is available, the explicit INFORMATION_SCHEMA test is skipped.

DB2 and MySQL live catalog tests
================================

Configure ``db2.url``, ``db2.user``, ``db2.password`` and/or corresponding ``mysql.*`` properties
in the same private file. Run::

    ./gradlew test --tests '*Db2MySqlMetaDataTest'

Fixtures use uniquely named tables in the DB2 login schema or MySQL current database and are
dropped after each test. Tests cover keys, comments, column details, indexes, JSON, unqualified
lineage and transaction preservation. MySQL bulk key call counts must stay constant as tables
are added; DB2 uses its supported per-table JDBC key fallback. Default test drivers are DB2 JCC
11.5.7.0 and MySQL Connector/J 8.2.0, overridable with ``-Pdb2JdbcVersion=...`` and
``-PmysqlJdbcVersion=...``. These drivers are not bundled into the transpiler JAR.



Catalog-only qualified names
----------------------------

For MySQL and MariaDB, two-part ``database.table`` names follow the extracted JDBC layout:
catalog and empty schema in catalog mode, or schema in schema mode (including Connector/J
``databaseTerm=SCHEMA``). Connector/J's synthetic ``def`` catalog in schema mode is
normalized to an empty catalog. Existing named schemas take precedence over catalog
rewriting. Backquoted names and database-qualified columns use the same mapping. Other database
profiles retain schema-qualified names; a qualifier can fall back to a catalog when it is not a
schema of the current catalog and that catalog has only an empty populated schema. Lineage
``table`` and ``scope`` names omit empty schema segments, for example ``test.lt_customers``.

Expression-aware lineage
------------------------

``JSONObjectTreeBuilder``, ``JsonTreeBuilder`` and ``XmlTreeBuilder`` expose additive
lineage attributes without changing existing names, aliases or physical column qualifiers:

* ``kind`` distinguishes columns, literals, parameters, functions, operators, CASE and scalar
  subqueries. Literals carry their SQL ``value`` and ``literalType``; parameters carry a
  statement-order ``index`` or ``parameterName``.
* ``expression`` contains JSqlParser's normalized SQL for expressions. Physical column
  references and star expansion omit it. References to derived columns include their reference
  text and a ``definition`` describing the nearest defining select item. Defining trees remain
  nested, including operators and aggregates, through successive CTEs and subqueries.
* Functions include ``function`` and recognized built-in aggregates include ``aggregate=true``.
  Analytic functions with OVER include ``window=true``. User-defined aggregate semantics cannot
  be inferred from the SQL syntax alone.
* ``role=condition`` marks CASE switches/WHEN conditions, aggregate FILTER predicates, NULLIF's
  second argument and IF/IIF conditions. CASE results use ``role=value``. Window partition and
  ordering expressions use ``role=partition`` and ``role=order``. An omitted role means value;
  consumers propagate each role through its subtree, including derived definitions.

Views registered with ``JdbcMetaData.put(resultSetMetaData, name, errorMessage)`` retain their
known defining SQL and lineage. Views described only by JDBC columns have no definition.
The scan does not fetch view SQL automatically. WHERE clauses are not added to the result-column
lineage; positional parameter indexes still reflect their position in the parsed statement.

``FlattenedColumnBuilder`` keeps its existing ``Map<String, Set<String>>`` of physical column
dependencies. After conversion, ``getColumnAttributes()`` provides the same top-level attributes
keyed by result label, including ``expression`` and ``definition``. Literal and parameter leaves
are excluded from the physical dependency sets. JSON object output retains its nested child
arrays; JSON text output retains its flat child arrays. SQL text is escaped for both JSON and XML.

Keys and catalog details
------------------------

The existing connection constructors retain their scan cost and compact catalog JSON. For an
export with keys and column details, opt in using::

    JdbcMetaData metadata = new JdbcMetaData(connection, List.of("public"),
        JdbcMetaDataOptions.defaults());

Defaults enable primary keys, imported foreign keys, comments and column details. Indexes are
more expensive and are disabled by default; enable them with ``setIndices(true)``. Each setting
has a fluent ``set...``/``with...`` method. ``JdbcMetaDataOptions.none()`` disables enrichment;
null options select defaults. The constructor snapshots the settings, so later mutations do not
change its output. ``getExtractionOptions()`` returns a copy.

Keys are loaded only for retained schemas. INFORMATION_SCHEMA queries read primary keys in bulk
for PostgreSQL, MySQL, SQL Server, H2 and DuckDB. Snowflake uses one
``SHOW PRIMARY KEYS IN SCHEMA`` and one ``SHOW IMPORTED KEYS IN SCHEMA`` per schema,
with safely quoted database/schema names, and never queries ``KEY_COLUMN_USAGE``. Oracle uses ALL_CONSTRAINTS and
ALL_CONS_COLUMNS. PostgreSQL's JDBC driver supplies imported keys with a null table in one call
per schema. Other INFORMATION_SCHEMA paths join referential constraints to ordered column pairs;
SQL Server uses sys views for foreign keys because its INFORMATION_SCHEMA lacks the referenced
column position. Failed bulk paths try schema-wide JDBC primary/imported keys with a null
table first, then per-table JDBC only if the schema-wide call fails. Empty results are
valid and do not trigger fallback. Db2 LUW reads imported keys in bulk from
``SYSCAT.REFERENCES`` and ``SYSCAT.KEYCOLUSE`` because its JDBC null-table call
silently returns an empty result. Indexes use approximate ``getIndexInfo`` per table when enabled.

A schema-wide native column read supplements identity/generated flags when the generic
INFORMATION_SCHEMA mapping cannot infer them. PostgreSQL keeps its existing JDBC column details.
Dialect comment queries supplement existing JDBC remarks where available. Unsupported comment
queries retain the driver's remarks. Speculative queries are guarded by savepoints inside caller
transactions; without savepoint support they are skipped. Recovery failures propagate, and the
scan never commits or changes auto-commit. Metadata availability still depends on database/driver
support and the caller's privileges.

``JdbcTable.primaryKey`` is null for tables without a primary key. Unique constraints do not
become primary keys. ``JdbcTable.foreignKeys`` holds imported keys, with column pairs in KEY_SEQ
order as ``[foreignColumn, referencedColumn]``. Referenced catalog/schema/table names are kept even
when that target is outside the schema filter. Key/index objects and index columns have public
getters. Metadata copies preserve these fields independently.

Catalog JSON adds table ``remarks``, ``primaryKey``, ``foreignKeys`` and optional ``indices``.
Column details add ``ordinalPosition``, ``remarks``, ``default``, ``autoIncrement`` and ``generated``
when known. Columns and key columns are serialized in their respective ordinal order.
Referential rules use CASCADE, RESTRICT, SET_NULL, NO_ACTION or SET_DEFAULT. Additional index
column details and foreign-key deferrability/referenced-key names preserve the full model on a
JSON round trip. Readers continue to ignore unknown fields and read catalogs from older versions.
Unknown ``isNullable`` is omitted and reads back as ``""`` / JDBC columnNullableUnknown; YES and
NO are emitted as true and false. Legacy constructor JSON keeps its original fields, with this
unknown-nullability correction.
