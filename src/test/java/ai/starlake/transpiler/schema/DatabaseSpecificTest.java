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

import ai.starlake.transpiler.schema.JdbcUtils.DatabaseSpecific;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.StringReader;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.*;

class DatabaseSpecificTest {
  @ParameterizedTest
  @CsvSource({"HDB,SAP_HANA", "SAP HANA,SAP_HANA", "SAP HANA Cloud,SAP_HANA", "Teradata,TERADATA",
      "Teradata Database,TERADATA", "DB2/LINUXX8664,DB2", "DB2 for z/OS,DB2",
      "DB2 UDB for AS/400,DB2", "MariaDB,MARIADB", "MySQL MariaDB,MARIADB",
      "Google BigQuery,BIGQUERY", "Google Big Query,BIGQUERY", "Amazon Redshift,REDSHIFT",
      "Amazon Redshift PostgreSQL,REDSHIFT", "Databricks,DATABRICKS", "SparkSQL,DATABRICKS",
      "Spark SQL,DATABRICKS", "Oracle,ORACLE", "PostgreSQL,POSTGRESQL",
      "Microsoft SQL Server,MSSQL", "MySQL,MYSQL", "Snowflake,SNOWFLAKE", "DuckDB,DUCKDB", "H2,H2",
      "SQLite,OTHER"})
  void detectsDriverProductNames(String product, DatabaseSpecific expected) {
    assertEquals(expected, DatabaseSpecific.getType(product));
    assertEquals(expected, DatabaseSpecific.getType(product.toLowerCase(Locale.ROOT)));
  }

  @Test
  void detectionDoesNotDependOnDefaultLocale() {
    Locale previous = Locale.getDefault();
    try {
      Locale.setDefault(Locale.forLanguageTag("tr-TR"));
      assertEquals(DatabaseSpecific.BIGQUERY, DatabaseSpecific.getType("bigquery"));
      assertEquals(DatabaseSpecific.MARIADB, DatabaseSpecific.getType("mariadb"));
    } finally {
      Locale.setDefault(previous);
    }
    assertEquals(DatabaseSpecific.OTHER, DatabaseSpecific.getType(null));
  }

  @ParameterizedTest
  @CsvSource({"SAP_HANA,SYS", "SAP_HANA,SYS_DATABASES", "SAP_HANA,_SYS_STATISTICS",
      "SAP_HANA,_SYS_REPO", "TERADATA,DBC", "TERADATA,Sys_Calendar", "DB2,SYSCAT", "DB2,SYSIBM",
      "DB2,SYSIBMADM", "DB2,SYSSTAT", "DB2,SYSPROC", "DB2,SYSFUN", "MARIADB,information_schema",
      "MARIADB,mysql", "MARIADB,performance_schema", "MARIADB,sys", "BIGQUERY,INFORMATION_SCHEMA",
      "REDSHIFT,pg_catalog", "REDSHIFT,pg_internal", "REDSHIFT,pg_toast",
      "REDSHIFT,information_schema", "REDSHIFT,pg_automv", "DATABRICKS,information_schema"})
  void excludesSystemSchemasIgnoringCase(DatabaseSpecific type, String schema) {
    assertFalse(type.processSchema(schema));
    assertFalse(type.processSchema(schema.toLowerCase(Locale.ROOT)));
    assertFalse(type.processSchema(schema.toUpperCase(Locale.ROOT)));
  }

  @ParameterizedTest
  @EnumSource(value = DatabaseSpecific.class,
      names = {"SAP_HANA", "TERADATA", "DB2", "MARIADB", "BIGQUERY", "REDSHIFT", "DATABRICKS"})
  void preservesBusinessSchemasAndJsonType(DatabaseSpecific type) {
    for (String schema : new String[] {"", "SALES", "PUBLIC", "DEFAULT", "SYS_SALES", "PG_SALES",
        "_SYS_BIC", "_SYS_BI"}) {
      assertTrue(type.processSchema(schema), type + ": " + schema);
    }
    // Ask the driver for all supported types, including external and materialized objects.
    assertNull(type.getTableTypes());
    JdbcMetaData metadata = new JdbcMetaData("catalog", "schema");
    metadata.setDatabaseType(type.name());
    JdbcMetaData restored = JdbcJSONSerializer
        .fromJson(new StringReader(JdbcJSONSerializer.toJson(metadata).toString()));
    assertEquals(type.name(), restored.getDatabaseType());
    assertEquals("catalog", restored.getCurrentCatalogName());
    assertEquals("schema", restored.getCurrentSchemaName());
  }
}
