/**
 * Starlake.AI JSQLTranspiler is a SQL to DuckDB Transpiler.
 * Copyright (C) 2024 Starlake.AI
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.starlake.transpiler;

import ai.starlake.transpiler.schema.JdbcColumn;
import ai.starlake.transpiler.schema.JdbcMetaData;
import ai.starlake.transpiler.schema.JdbcResultSetMetaData;
import ai.starlake.transpiler.schema.JdbcTable;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Database;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.ParenthesedFromItem;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.TableFunction;
import net.sf.jsqlparser.statement.select.TableStatement;
import net.sf.jsqlparser.statement.select.Values;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.util.deparser.StatementDeParser;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * A class for resolving the actual columns returned by a SELECT statement. Depends on virtual or
 * physical Database Metadata holding the schema and table information.
 */
@SuppressWarnings({"PMD.CyclomaticComplexity"})
public class JSQLColumResolver
    implements SelectVisitor<JdbcResultSetMetaData>, FromItemVisitor<JdbcResultSetMetaData> {
  public final static Logger LOGGER = Logger.getLogger(JSQLColumResolver.class.getName());

  private final String currentCatalogName;
  private final String currentSchemaName;

  private final JdbcMetaData metaData;
  private final JSQLExpressionColumnResolver expressionColumnResolver;

  /**
   * Instantiates a new JSQLColumnResolver for the provided Meta Data.
   *
   * @param currentCatalogName the current catalog name, can be empty
   * @param currentSchemaName the current schema name, can be empty
   * @param metaData the database meta data holding the catalogs, schemas and table definitions
   */
  public JSQLColumResolver(String currentCatalogName, String currentSchemaName,
      JdbcMetaData metaData) {
    this.currentCatalogName = currentCatalogName;
    this.currentSchemaName = currentSchemaName;
    this.metaData = metaData;

    this.expressionColumnResolver = new JSQLExpressionColumnResolver(this);
  }


  /**
   * Instantiates a new JSQLColumnResolver for the provided Meta Data with an empty CURRENT_SCHEMA
   * and CURRENT_CATALOG
   *
   * @param metaData the meta data
   */
  public JSQLColumResolver(JdbcMetaData metaData) {
    this("", "", metaData);
  }


  /**
   * Instantiates a new JSQLColumnResolver for the provided simplified Meta Data, presented as an
   * Array of Tables and Column Names only.
   *
   * @param currentCatalogName the current catalog name
   * @param currentSchemaName the current schema name
   * @param metaDataDefinition the meta data definition as n Array of Tablename and Column Names
   */
  public JSQLColumResolver(String currentCatalogName, String currentSchemaName,
      String[][] metaDataDefinition) {
    this(currentCatalogName, currentSchemaName, new JdbcMetaData(metaDataDefinition));
  }

  /**
   * Instantiates a new JSQLColumnResolver for the provided simplified Meta Data with an empty
   * CURRENT_SCHEMA and CURRENT_CATALOG
   *
   * @param metaDataDefinition the meta data definition as n Array of Tablename and Column Names
   */
  public JSQLColumResolver(String[][] metaDataDefinition) {
    this("", "", metaDataDefinition);
  }


  /**
   * Resolves the actual columns returned by a SELECT statement for a given CURRENT_CATALOG and
   * CURRENT_SCHEMA and wraps this information into `ResultSetMetaData`.
   *
   * @param sqlStr the `SELECT` statement text
   * @param metaData the Database Meta Data
   * @param currentCatalogName the CURRENT_CATALOG name (which is the default catalog for accessing
   *        the schemas)
   * @param currentSchemaName the CURRENT_SCHEMA name (which is the default schema for accessing the
   *        tables)
   * @return the ResultSetMetaData representing the actual columns returned by the `SELECT`
   *         statement
   * @throws JSQLParserException when the `SELECT` statement text can not be parsed
   */
  @SuppressWarnings({"PMD.CyclomaticComplexity", "PMD.ExcessiveMethodLength"})
  public static JdbcResultSetMetaData getResultSetMetaData(String sqlStr, JdbcMetaData metaData,
      String currentCatalogName, String currentSchemaName) throws JSQLParserException {
    if (sqlStr == null || sqlStr.trim().isEmpty()) {
      throw new JSQLParserException("The provided Statement must not be empty.");
    }

    JSQLColumResolver resolver =
        new JSQLColumResolver(currentCatalogName, currentSchemaName, metaData);

    Statement st = CCJSqlParserUtil.parse(sqlStr);
    if (st instanceof Select) {
      PlainSelect select = (PlainSelect) st;
      return select.accept(resolver, JdbcMetaData.copyOf(metaData));

    } else {
      throw new RuntimeException(
          st.getClass().getSimpleName() + " Statements are not supported yet.");
    }
  }

  /**
   * Resolves the actual columns returned by a SELECT statement for a given CURRENT_CATALOG and
   * CURRENT_SCHEMA and wraps this information into `ResultSetMetaData`.
   *
   * @param sqlStr the `SELECT` statement text
   * @param metaDataDefinition the meta data definition as an array of Tables with Columns e.g. {
   *        TABLE_NAME, COLUMN1, COLUMN2 ... COLUMN10 }
   * @param currentCatalogName the CURRENT_CATALOG name (which is the default catalog for accessing
   *        the schemas)
   * @param currentSchemaName the CURRENT_SCHEMA name (which is the default schema for accessing the
   *        tables)
   * @return the ResultSetMetaData representing the actual columns returned by the `SELECT`
   *         statement
   * @throws JSQLParserException when the `SELECT` statement text can not be parsed
   */
  public static JdbcResultSetMetaData getResultSetMetaData(String sqlStr,
      String[][] metaDataDefinition, String currentCatalogName, String currentSchemaName)
      throws JSQLParserException {
    JdbcMetaData metaData =
        new JdbcMetaData(currentCatalogName, currentSchemaName, metaDataDefinition);
    return getResultSetMetaData(sqlStr, metaData, currentCatalogName, currentSchemaName);
  }

  /**
   * Resolves the actual columns returned by a SELECT statement for an empty CURRENT_CATALOG and and
   * empty CURRENT_SCHEMA and wraps this information into `ResultSetMetaData`.
   *
   * @param sqlStr the `SELECT` statement text
   * @param metaDataDefinition the meta data definition as an array of Tables with Columns e.g. {
   *        TABLE_NAME, COLUMN1, COLUMN2 ... COLUMN10 }
   * @return the ResultSetMetaData representing the actual columns returned by the `SELECT`
   *         statement
   * @throws JSQLParserException when the `SELECT` statement text can not be parsed
   */
  public JdbcResultSetMetaData getResultSetMetaData(String sqlStr, String[][] metaDataDefinition)
      throws JSQLParserException {
    JdbcMetaData metaData =
        new JdbcMetaData(currentCatalogName, currentSchemaName, metaDataDefinition);
    return getResultSetMetaData(sqlStr, metaData, "", "");
  }

  /**
   * Resolves the actual columns returned by a SELECT statement for an empty CURRENT_CATALOG and an
   * empty CURRENT_SCHEMA and wraps this information into `ResultSetMetaData`.
   *
   * @param sqlStr the `SELECT` statement text
   * @return the ResultSetMetaData representing the actual columns returned by the `SELECT`
   *         statement
   * @throws JSQLParserException when the `SELECT` statement text can not be parsed
   */
  public JdbcResultSetMetaData getResultSetMetaData(String sqlStr) throws JSQLParserException {

    Statement st = CCJSqlParserUtil.parse(sqlStr);
    if (st instanceof Select) {
      Select select = (Select) st;
      return select.accept(this, JdbcMetaData.copyOf(metaData));
    } else {
      throw new RuntimeException("Unsupported Statement");
    }
  }


  /**
   * Gets the rewritten statement text with any AllColumns "*" or AllTableColumns "t.*" expression
   * resolved into the actual columns
   *
   * @param sqlStr the query statement string (using any AllColumns "*" or AllTableColumns "t.*"
   *        expression)
   * @return rewritten statement text with any AllColumns "*" or AllTableColumns "t.*" expression
   *         resolved into the actual columns
   * @throws JSQLParserException the exception when parsing the query statement fails
   */
  public String getResolvedStatementText(String sqlStr) throws JSQLParserException {
    StringBuilder builder = new StringBuilder();
    StatementDeParser deParser = new StatementDeParser(builder);

    Statement st = CCJSqlParserUtil.parse(sqlStr);
    if (st instanceof Select) {
      Select select = (Select) st;
      select.accept(this, JdbcMetaData.copyOf(metaData));
    }
    st.accept(deParser);
    return builder.toString();
  }


  @Override
  public <S> JdbcResultSetMetaData visit(Table table, S s) {
    JdbcResultSetMetaData rsMetaData = new JdbcResultSetMetaData();

    if (table.getSchemaName() == null || table.getSchemaName().isEmpty()) {
      table.setSchemaName(currentSchemaName);
    }

    if (table.getDatabase() == null) {
      table.setDatabase(new Database(currentCatalogName));
    } else if (table.getDatabase().getDatabaseName() == null
        || table.getDatabase().getDatabaseName().isEmpty()) {
      table.getDatabase().setDatabaseName(currentCatalogName);
    }

    for (JdbcColumn jdbcColumn : metaData.getTableColumns(table.getDatabase().getDatabaseName(),
        table.getSchemaName(), table.getName(), null)) {

      rsMetaData.add(jdbcColumn, null);
    }

    return rsMetaData;
  }

  @Override
  public void visit(Table tableName) {
    FromItemVisitor.super.visit(tableName);
  }

  public JdbcResultSetMetaData visit(ParenthesedSelect parenthesedSelect, JdbcMetaData parameters) {
    JdbcResultSetMetaData rsMetaData = null;
    Alias alias = parenthesedSelect.getAlias();
    JdbcTable t = new JdbcTable(currentCatalogName, currentSchemaName,
        alias != null ? parenthesedSelect.getAlias().getName() : "");

    rsMetaData = parenthesedSelect.getSelect().accept((SelectVisitor<JdbcResultSetMetaData>) this,
        JdbcMetaData.copyOf(parameters));
    try {
      int columnCount = rsMetaData.getColumnCount();
      for (int i = 1; i <= columnCount; i++) {
        t.add(t.tableCatalog, t.tableSchema, t.tableName, rsMetaData.getColumnName(i),
            rsMetaData.getColumnType(i), rsMetaData.getColumnClassName(i),
            rsMetaData.getPrecision(i), rsMetaData.getScale(i), 10, rsMetaData.isNullable(i), "",
            "", rsMetaData.getColumnDisplaySize(i), i, "", rsMetaData.getCatalogName(i),
            rsMetaData.getSchemaName(i), rsMetaData.getTableName(i), null, "", "");
      }
      metaData.put(t);
    } catch (SQLException ex) {
      throw new RuntimeException("Error in WITH clause " + parenthesedSelect.toString(), ex);
    }
    return rsMetaData;
  }

  @Override
  public <S> JdbcResultSetMetaData visit(ParenthesedSelect parenthesedSelect, S parameters) {
    if (parameters instanceof JdbcMetaData) {
      JdbcMetaData metaData1 = (JdbcMetaData) parameters;
      return visit(parenthesedSelect, JdbcMetaData.copyOf(metaData1));
    }
    return null;
  }

  @Override
  public void visit(ParenthesedSelect parenthesedSelect) {
    SelectVisitor.super.visit(parenthesedSelect);
  }


  @SuppressWarnings({"PMD.CyclomaticComplexity", "PMD.ExcessiveMethodLength"})
  public JdbcResultSetMetaData visit(PlainSelect select, JdbcMetaData metaData) {
    JdbcResultSetMetaData resultSetMetaData = new JdbcResultSetMetaData();

    FromItem fromItem = select.getFromItem();
    List<Join> joins = select.getJoins();

    if (select.getWithItemsList() != null) {
      for (WithItem withItem : select.getWithItemsList()) {
        withItem.accept((SelectVisitor<JdbcResultSetMetaData>) this, metaData);
      }
    }

    if (fromItem instanceof Table) {
      Alias alias = fromItem.getAlias();
      Table t = (Table) fromItem;

      if (alias != null) {
        metaData.getFromTables().put(alias.getName(), (Table) fromItem);
      } else {
        metaData.getFromTables().put(t.getName(), (Table) fromItem);
      }
    } else if (fromItem != null) {
      Alias alias = fromItem.getAlias();
      JdbcTable t = new JdbcTable(currentCatalogName, currentSchemaName,
          alias != null ? alias.getName() : "");

      JdbcResultSetMetaData rsMetaData = fromItem.accept(this, JdbcMetaData.copyOf(metaData));
      try {
        int columnCount = rsMetaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
          t.add(t.tableCatalog, t.tableSchema, t.tableName, rsMetaData.getColumnName(i),
              rsMetaData.getColumnType(i), rsMetaData.getColumnClassName(i),
              rsMetaData.getPrecision(i), rsMetaData.getScale(i), 10, rsMetaData.isNullable(i), "",
              "", rsMetaData.getColumnDisplaySize(i), i, "", rsMetaData.getCatalogName(i),
              rsMetaData.getSchemaName(i), rsMetaData.getTableName(i), null, "", "");
        }
        metaData.put(t);
        metaData.getFromTables().put(alias != null ? alias.getName() : t.tableName,
            new Table(alias != null ? alias.getName() : t.tableName));
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    if (joins != null) {
      for (Join join : joins) {
        if (join.getFromItem() instanceof Table) {
          Alias alias = join.getFromItem().getAlias();
          Table t = (Table) join.getFromItem();

          if (alias != null) {
            metaData.getFromTables().put(alias.getName(), t);
          } else {
            metaData.getFromTables().put(t.getName(), t);
          }
        } else {
          Alias alias = join.getFromItem().getAlias();
          JdbcTable t = new JdbcTable(currentCatalogName, currentSchemaName,
              alias != null ? alias.getName() : "");

          JdbcResultSetMetaData rsMetaData =
              join.getFromItem().accept(this, JdbcMetaData.copyOf(metaData));
          try {
            int columnCount = rsMetaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
              t.add(t.tableCatalog, t.tableSchema, t.tableName, rsMetaData.getColumnName(i),
                  rsMetaData.getColumnType(i), rsMetaData.getColumnClassName(i),
                  rsMetaData.getPrecision(i), rsMetaData.getScale(i), 10, rsMetaData.isNullable(i),
                  "", "", rsMetaData.getColumnDisplaySize(i), i, "", rsMetaData.getCatalogName(i),
                  rsMetaData.getSchemaName(i), rsMetaData.getTableName(i), null, "", "");
            }
            metaData.put(t);
            metaData.getFromTables().put(alias != null ? alias.getName() : t.tableName,
                new Table(alias != null ? alias.getName() : t.tableName));
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }

    for (Table t : metaData.getFromTables().values()) {
      if (t.getSchemaName() == null || t.getSchemaName().isEmpty()) {
        t.setSchemaName(currentSchemaName);
      }

      if (t.getDatabase() == null) {
        t.setDatabase(new Database(currentCatalogName));
      } else if (t.getDatabase().getDatabaseName() == null
          || t.getDatabase().getDatabaseName().isEmpty()) {
        t.getDatabase().setDatabaseName(currentCatalogName);
      }
    }

    /* this is valid SQL:
    
    SELECT
        main.sales.salesid
    FROM main.sales
     */

    // column positions in MetaData start at 1
    ArrayList<SelectItem<?>> newSelectItems = new ArrayList<>();
    for (SelectItem<?> selectItem : select.getSelectItems()) {
      Alias alias = selectItem.getAlias();
      List<JdbcColumn> jdbcColumns =
          selectItem.getExpression().accept(expressionColumnResolver, metaData);

      for (JdbcColumn col : jdbcColumns) {
        resultSetMetaData.add(col, alias != null ? alias.withUseAs(false).getName() : null);

        Table t = new Table(col.tableCatalog, col.tableSchema, col.tableName);
        newSelectItems.add(new SelectItem<>(
            new Column(t, col.columnName).withCommentText("Resolved Column"), alias));
      }
    }
    select.setSelectItems(newSelectItems);

    return resultSetMetaData;
  }

  @Override
  public <S> JdbcResultSetMetaData visit(PlainSelect select, S context) {
    if (context instanceof JdbcMetaData) {
      return visit(select, (JdbcMetaData) context);
    } else {
      return null;
    }
  }

  @Override
  public void visit(PlainSelect plainSelect) {
    SelectVisitor.super.visit(plainSelect);
  }

  @Override
  public <S> JdbcResultSetMetaData visit(SetOperationList setOperationList, S context) {
    return null;
  }

  @Override
  public void visit(SetOperationList setOpList) {
    SelectVisitor.super.visit(setOpList);
  }


  @Override
  public <S> JdbcResultSetMetaData visit(WithItem withItem, S context) {
    if (context instanceof JdbcMetaData) {
      JdbcMetaData metaData = (JdbcMetaData) context;

      JdbcTable t =
          new JdbcTable(currentCatalogName, currentSchemaName, withItem.getAlias().getName());

      JdbcResultSetMetaData md = withItem.getSelect()
          .accept((SelectVisitor<JdbcResultSetMetaData>) this, JdbcMetaData.copyOf(metaData));
      try {
        int columnCount = md.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
          t.add(t.tableCatalog, t.tableSchema, t.tableName, md.getColumnName(i),
              md.getColumnType(i), md.getColumnClassName(i), md.getPrecision(i), md.getScale(i), 10,
              md.isNullable(i), "", "", md.getColumnDisplaySize(i), i, "", md.getCatalogName(i),
              md.getSchemaName(i), md.getTableName(i), null, "", "");
        }
        metaData.put(t);
      } catch (SQLException ex) {
        throw new RuntimeException("Error in WITH clause " + withItem.toString(), ex);
      }
    }

    return withItem.getSelect().accept((SelectVisitor<JdbcResultSetMetaData>) this, metaData);
  }

  @Override
  public void visit(WithItem withItem) {
    SelectVisitor.super.visit(withItem);
  }


  @Override
  public <S> JdbcResultSetMetaData visit(Values values, S context) {
    return null;
  }

  @Override
  public void visit(Values values) {
    SelectVisitor.super.visit(values);
  }

  @Override
  public <S> JdbcResultSetMetaData visit(LateralSubSelect lateralSubSelect, S context) {
    return null;
  }

  @Override
  public void visit(LateralSubSelect lateralSubSelect) {
    SelectVisitor.super.visit(lateralSubSelect);
  }

  @Override
  public <S> JdbcResultSetMetaData visit(TableFunction tableFunction, S context) {
    return null;
  }

  @Override
  public void visit(TableFunction tableFunction) {
    FromItemVisitor.super.visit(tableFunction);
  }

  @Override
  public <S> JdbcResultSetMetaData visit(ParenthesedFromItem parenthesedFromItem, S context) {
    JdbcResultSetMetaData resultSetMetaData = new JdbcResultSetMetaData();

    FromItem fromItem = parenthesedFromItem.getFromItem();
    try {
      resultSetMetaData.add(fromItem.accept(this, context));
    } catch (SQLException ex) {
      throw new RuntimeException("Failed on ParenthesedFromItem " + fromItem.toString(), ex);
    }

    List<Join> joins = parenthesedFromItem.getJoins();
    if (joins != null && !joins.isEmpty()) {
      for (Join join : joins) {
        try {
          resultSetMetaData.add(join.getFromItem().accept(this, context));
        } catch (SQLException ex) {
          throw new RuntimeException("Failed on Join " + join.getFromItem().toString(), ex);
        }
      }
    }
    return resultSetMetaData;
  }

  @Override
  public void visit(ParenthesedFromItem parenthesedFromItem) {
    FromItemVisitor.super.visit(parenthesedFromItem);
  }

  @Override
  public <S> JdbcResultSetMetaData visit(TableStatement tableStatement, S context) {
    return null;
  }

  @Override
  public void visit(TableStatement tableStatement) {
    SelectVisitor.super.visit(tableStatement);
  }

}
