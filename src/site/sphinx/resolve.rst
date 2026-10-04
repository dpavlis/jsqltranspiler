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
and doubled double quotes represent a literal quote. Catalog wildcards are rejected.
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
pg_catalog, pg_internal, pg_toast, pg_automv and catalog_history. Existing database policies
are unchanged.

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

