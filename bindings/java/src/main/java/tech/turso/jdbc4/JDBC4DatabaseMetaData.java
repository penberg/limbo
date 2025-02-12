package tech.turso.jdbc4;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import tech.turso.annotations.Nullable;
import tech.turso.annotations.SkipNullableCheck;
import tech.turso.utils.Logger;
import tech.turso.utils.LoggerFactory;

public class JDBC4DatabaseMetaData implements DatabaseMetaData {

  private static final Logger logger = LoggerFactory.getLogger(JDBC4DatabaseMetaData.class);

  private static String driverName = "";
  private static String driverVersion = "";

  private final JDBC4Connection connection;
  @Nullable private PreparedStatement getTables = null;
  @Nullable private PreparedStatement getTableTypes = null;
  @Nullable private PreparedStatement getTypeInfo = null;
  @Nullable private PreparedStatement getCatalogs = null;
  @Nullable private PreparedStatement getSchemas = null;
  @Nullable private PreparedStatement getUDTs = null;
  @Nullable private PreparedStatement getColumnsTblName = null;
  @Nullable private PreparedStatement getSuperTypes = null;
  @Nullable private PreparedStatement getSuperTables = null;
  @Nullable private PreparedStatement getTablePrivileges = null;
  @Nullable private PreparedStatement getIndexInfo = null;
  @Nullable private PreparedStatement getProcedures = null;
  @Nullable private PreparedStatement getProcedureColumns = null;
  @Nullable private PreparedStatement getAttributes = null;
  @Nullable private PreparedStatement getBestRowIdentifier = null;
  @Nullable private PreparedStatement getVersionColumns = null;
  @Nullable private PreparedStatement getColumnPrivileges = null;

  static {
    try (InputStream limboJdbcPropStream =
        JDBC4DatabaseMetaData.class.getClassLoader().getResourceAsStream("limbo-jdbc.properties")) {
      if (limboJdbcPropStream == null) {
        throw new IOException("Cannot load limbo-jdbc.properties from jar");
      }

      final Properties properties = new Properties();
      properties.load(limboJdbcPropStream);
      driverName = properties.getProperty("driverName");
      driverVersion = properties.getProperty("driverVersion");
    } catch (IOException e) {
      logger.error("Failed to load driverName and driverVersion");
    }
  }

  public JDBC4DatabaseMetaData(JDBC4Connection connection) {
    this.connection = connection;
  }

  @Override
  public boolean allProceduresAreCallable() {
    return false;
  }

  @Override
  public boolean allTablesAreSelectable() {
    return true;
  }

  @Override
  public String getURL() {
    return connection.getUrl();
  }

  @Override
  @Nullable
  public String getUserName() {
    return null;
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return connection.isReadOnly();
  }

  @Override
  public boolean nullsAreSortedHigh() {
    return true;
  }

  @Override
  public boolean nullsAreSortedLow() {
    return !nullsAreSortedHigh();
  }

  @Override
  public boolean nullsAreSortedAtStart() {
    return true;
  }

  @Override
  public boolean nullsAreSortedAtEnd() {
    return !nullsAreSortedAtStart();
  }

  @Override
  public String getDatabaseProductName() {
    return "Limbo";
  }

  @Override
  public String getDatabaseProductVersion() {
    // TODO
    return "";
  }

  @Override
  public String getDriverName() {
    return driverName;
  }

  @Override
  public String getDriverVersion() {
    return driverVersion;
  }

  @Override
  public int getDriverMajorVersion() {
    return Integer.parseInt(driverVersion.split("\\.")[0]);
  }

  @Override
  public int getDriverMinorVersion() {
    return Integer.parseInt(driverVersion.split("\\.")[1]);
  }

  @Override
  public boolean usesLocalFiles() {
    return true;
  }

  @Override
  public boolean usesLocalFilePerTable() {
    return false;
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() {
    return true;
  }

  @Override
  public boolean storesUpperCaseIdentifiers() {
    return false;
  }

  @Override
  public boolean storesLowerCaseIdentifiers() {
    return false;
  }

  @Override
  public boolean storesMixedCaseIdentifiers() {
    return true;
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() {
    return false;
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() {
    return false;
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() {
    return false;
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() {
    return false;
  }

  @Override
  public String getIdentifierQuoteString() {
    return "\"";
  }

  @Override
  public String getSQLKeywords() {
    // TODO: add more limbo supported keywords
    return "ABORT,ACTION,AFTER,ANALYZE,ATTACH,AUTOINCREMENT,BEFORE,"
        + "CASCADE,CONFLICT,DATABASE,DEFERRABLE,DEFERRED,DESC,DETACH,"
        + "EXCLUSIVE,EXPLAIN,FAIL,GLOB,IGNORE,INDEX,INDEXED,INITIALLY,INSTEAD,ISNULL,"
        + "KEY,LIMIT,NOTNULL,OFFSET,PLAN,PRAGMA,QUERY,"
        + "RAISE,REGEXP,REINDEX,RENAME,REPLACE,RESTRICT,"
        + "TEMP,TEMPORARY,TRANSACTION,VACUUM,VIEW,VIRTUAL";
  }

  @Override
  public String getNumericFunctions() {
    // TODO
    return "";
  }

  @Override
  public String getStringFunctions() {
    // TOOD
    return "";
  }

  @Override
  public String getSystemFunctions() {
    // TODO
    return "";
  }

  @Override
  public String getTimeDateFunctions() {
    // TODO
    return "";
  }

  @Override
  public String getSearchStringEscape() {
    return "\\";
  }

  @Override
  public String getExtraNameCharacters() {
    return "";
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() {
    return false;
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() {
    return false;
  }

  @Override
  public boolean supportsColumnAliasing() {
    return true;
  }

  @Override
  public boolean nullPlusNonNullIsNull() {
    return true;
  }

  @Override
  public boolean supportsConvert() {
    return false;
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) {
    return false;
  }

  @Override
  public boolean supportsTableCorrelationNames() {
    return false;
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() {
    return false;
  }

  @Override
  public boolean supportsExpressionsInOrderBy() {
    return true;
  }

  @Override
  public boolean supportsOrderByUnrelated() {
    return false;
  }

  @Override
  public boolean supportsGroupBy() {
    return true;
  }

  @Override
  public boolean supportsGroupByUnrelated() {
    return false;
  }

  @Override
  public boolean supportsGroupByBeyondSelect() {
    return false;
  }

  @Override
  public boolean supportsLikeEscapeClause() {
    return false;
  }

  @Override
  public boolean supportsMultipleResultSets() {
    return false;
  }

  @Override
  public boolean supportsMultipleTransactions() {
    return true;
  }

  @Override
  public boolean supportsNonNullableColumns() {
    return true;
  }

  @Override
  public boolean supportsMinimumSQLGrammar() {
    // ODBC minimum grammar should be supported, which are the followings:
    //   SELECT
    //   INSERT
    //   UPDATE
    //   DELETE
    //   CREATE TABLE
    //   DROP TABLE
    //   GRANT
    //   REVOKE
    //   COMMIT
    //   ROLLBACK
    //   DECLARE CURSOR
    //   FETCH
    //   CLOSE CURSOR
    // TODO: Let's return true when limbo supports them all
    return false;
  }

  @Override
  public boolean supportsCoreSQLGrammar() {
    // supportsMinimumSQLGrammar() and some additional such as:
    //   Joins (INNER JOIN, OUTER JOIN, LEFT JOIN, RIGHT JOIN)
    //   Set operations (UNION, INTERSECT, EXCEPT)
    //   Subqueries (e.g., SELECT * FROM table WHERE column IN (SELECT column FROM another_table))
    //   Table expressions (SELECT column FROM (SELECT * FROM table) AS subquery)
    //   Data type support (includes more SQL data types like FLOAT, NUMERIC, DECIMAL)
    //   Basic string functions (e.g., LENGTH(), SUBSTRING(), CONCAT())
    // TODO: Let's return true when limbo supports them all
    return false;
  }

  @Override
  public boolean supportsExtendedSQLGrammar() {
    return false;
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() {
    return false;
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() {
    return false;
  }

  @Override
  public boolean supportsANSI92FullSQL() {
    return false;
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() {
    return false;
  }

  @Override
  public boolean supportsOuterJoins() {
    return true;
  }

  @Override
  public boolean supportsFullOuterJoins() {
    return true;
  }

  @Override
  public boolean supportsLimitedOuterJoins() {
    return true;
  }

  @Override
  public String getSchemaTerm() {
    return "schema";
  }

  @Override
  public String getProcedureTerm() {
    return "not_implemented";
  }

  @Override
  public String getCatalogTerm() {
    return "catalog";
  }

  @Override
  public boolean isCatalogAtStart() {
    // sqlite and limbo doesn't use catalog
    return false;
  }

  @Override
  public String getCatalogSeparator() {
    return ".";
  }

  @Override
  public boolean supportsSchemasInDataManipulation() {
    return false;
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() {
    return false;
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() {
    return false;
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() {
    return false;
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() {
    return false;
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() {
    return false;
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() {
    return false;
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() {
    return false;
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() {
    return false;
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() {
    return false;
  }

  @Override
  public boolean supportsPositionedDelete() {
    return false;
  }

  @Override
  public boolean supportsPositionedUpdate() {
    return false;
  }

  @Override
  public boolean supportsSelectForUpdate() {
    return false;
  }

  @Override
  public boolean supportsStoredProcedures() {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInComparisons() {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInExists() {
    return true;
  }

  @Override
  public boolean supportsSubqueriesInIns() {
    return true;
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() {
    return false;
  }

  @Override
  public boolean supportsCorrelatedSubqueries() {
    return false;
  }

  @Override
  public boolean supportsUnion() {
    // TODO: return true when limbo supports
    return false;
  }

  @Override
  public boolean supportsUnionAll() {
    // TODO: return true when limbo supports
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() {
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() {
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() {
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() {
    return false;
  }

  @Override
  public int getMaxBinaryLiteralLength() {
    return 0;
  }

  @Override
  public int getMaxCharLiteralLength() {
    return 0;
  }

  @Override
  public int getMaxColumnNameLength() {
    return 0;
  }

  @Override
  public int getMaxColumnsInGroupBy() {
    return 0;
  }

  @Override
  public int getMaxColumnsInIndex() {
    return 0;
  }

  @Override
  public int getMaxColumnsInOrderBy() {
    return 0;
  }

  @Override
  public int getMaxColumnsInSelect() {
    return 0;
  }

  @Override
  public int getMaxColumnsInTable() {
    return 0;
  }

  @Override
  public int getMaxConnections() {
    return 0;
  }

  @Override
  public int getMaxCursorNameLength() {
    return 0;
  }

  @Override
  public int getMaxIndexLength() {
    return 0;
  }

  @Override
  public int getMaxSchemaNameLength() {
    return 0;
  }

  @Override
  public int getMaxProcedureNameLength() {
    return 0;
  }

  @Override
  public int getMaxCatalogNameLength() {
    return 0;
  }

  @Override
  public int getMaxRowSize() {
    return 0;
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() {
    return false;
  }

  @Override
  public int getMaxStatementLength() {
    return 0;
  }

  @Override
  public int getMaxStatements() {
    return 0;
  }

  @Override
  public int getMaxTableNameLength() {
    return 0;
  }

  @Override
  public int getMaxTablesInSelect() {
    return 0;
  }

  @Override
  public int getMaxUserNameLength() {
    return 0;
  }

  @Override
  public int getDefaultTransactionIsolation() {
    // TODO: after limbo introduces Hekaton MVCC, what should we return?
    return Connection.TRANSACTION_SERIALIZABLE;
  }

  @Override
  public boolean supportsTransactions() {
    // TODO: limbo doesn't support transactions fully, let's return true when supported
    return false;
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level) {
    return Connection.TRANSACTION_SERIALIZABLE == level;
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() {
    // TODO: return true when supported
    return false;
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() {
    return false;
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() {
    return false;
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() {
    return false;
  }

  @Override
  @SkipNullableCheck
  public ResultSet getProcedures(
      String catalog, String schemaPattern, String procedureNamePattern) {
    // TODO
    return null;
  }

  @Override
  @SkipNullableCheck
  public ResultSet getProcedureColumns(
      String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) {
    // TODO
    return null;
  }

  @Override
  @SkipNullableCheck
  public ResultSet getTables(
      @Nullable String catalog,
      @Nullable String schemaPattern,
      String tableNamePattern,
      @Nullable String[] types)
      throws SQLException {
    // TODO: after union is supported
    return null;
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    if (getSchemas == null) {
      connection.checkOpen();
      getSchemas =
          connection.prepareStatement("select null as TABLE_SCHEM, null as TABLE_CATALOG limit 0;");
    }

    return getSchemas.executeQuery();
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    if (getCatalogs == null) {
      connection.checkOpen();
      getCatalogs = connection.prepareStatement("select null as TABLE_CAT limit 0;");
    }

    return getCatalogs.executeQuery();
  }

  @Override
  @SkipNullableCheck
  public ResultSet getTableTypes() {
    // TODO
    return null;
  }

  @Override
  @SkipNullableCheck
  public ResultSet getColumns(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) {
    // TODO - important
    return null;
  }

  @Override
  @SkipNullableCheck
  public ResultSet getColumnPrivileges(
      String catalog, String schema, String table, String columnNamePattern) throws SQLException {
    if (getColumnPrivileges == null) {
      connection.close();
      getColumnPrivileges =
          connection.prepareStatement(
              "select null as TABLE_CAT, null as TABLE_SCHEM, "
                  + "null as TABLE_NAME, null as COLUMN_NAME, null as GRANTOR, null as GRANTEE, "
                  + "null as PRIVILEGE, null as IS_GRANTABLE limit 0;");
    }

    return getColumnPrivileges.executeQuery();
  }

  @Override
  @SkipNullableCheck
  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    if (getTablePrivileges == null) {
      connection.checkOpen();
      getTablePrivileges =
          connection.prepareStatement(
              "select  null as TABLE_CAT, "
                  + "null as TABLE_SCHEM, null as TABLE_NAME, null as GRANTOR, null "
                  + "GRANTEE,  null as PRIVILEGE, null as IS_GRANTABLE limit 0;");
    }
    return getTablePrivileges.executeQuery();
  }

  @Override
  @SkipNullableCheck
  public ResultSet getBestRowIdentifier(
      String catalog, String schema, String table, int scope, boolean nullable)
      throws SQLException {
    if (getBestRowIdentifier == null) {
      connection.checkOpen();
      getBestRowIdentifier =
          connection.prepareStatement(
              "select null as SCOPE, null as COLUMN_NAME, "
                  + "null as DATA_TYPE, null as TYPE_NAME, null as COLUMN_SIZE, "
                  + "null as BUFFER_LENGTH, null as DECIMAL_DIGITS, null as PSEUDO_COLUMN limit 0;");
    }

    return getBestRowIdentifier.executeQuery();
  }

  @Override
  @SkipNullableCheck
  public ResultSet getVersionColumns(String catalog, String schema, String table)
      throws SQLException {
    if (getVersionColumns == null) {
      connection.close();
      getVersionColumns =
          connection.prepareStatement(
              "select null as SCOPE, null as COLUMN_NAME, "
                  + "null as DATA_TYPE, null as TYPE_NAME, null as COLUMN_SIZE, "
                  + "null as BUFFER_LENGTH, null as DECIMAL_DIGITS, null as PSEUDO_COLUMN limit 0;");
    }
    return getVersionColumns.executeQuery();
  }

  @Override
  @SkipNullableCheck
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) {
    // TODO - important
    return null;
  }

  @Override
  @SkipNullableCheck
  public ResultSet getImportedKeys(String catalog, String schema, String table) {
    // TODO
    return null;
  }

  @Override
  @SkipNullableCheck
  public ResultSet getExportedKeys(String catalog, String schema, String table) {
    return null;
  }

  @Override
  @SkipNullableCheck
  public ResultSet getCrossReference(
      String parentCatalog,
      String parentSchema,
      String parentTable,
      String foreignCatalog,
      String foreignSchema,
      String foreignTable) {
    // TODO
    return null;
  }

  @Override
  @SkipNullableCheck
  public ResultSet getTypeInfo() {
    // TODO
    return null;
  }

  @Override
  @SkipNullableCheck
  public ResultSet getIndexInfo(
      String catalog, String schema, String table, boolean unique, boolean approximate) {
    // TODO
    return null;
  }

  @Override
  public boolean supportsResultSetType(int type) {
    return type == ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency) {
    return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) {
    return false;
  }

  @Override
  public boolean ownDeletesAreVisible(int type) {
    return false;
  }

  @Override
  public boolean ownInsertsAreVisible(int type) {
    return false;
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) {
    return false;
  }

  @Override
  public boolean othersDeletesAreVisible(int type) {
    return false;
  }

  @Override
  public boolean othersInsertsAreVisible(int type) {
    return false;
  }

  @Override
  public boolean updatesAreDetected(int type) {
    return false;
  }

  @Override
  public boolean deletesAreDetected(int type) {
    return false;
  }

  @Override
  public boolean insertsAreDetected(int type) {
    return false;
  }

  @Override
  public boolean supportsBatchUpdates() {
    // TODO - let's add support for batch updates in the future and let this method return true
    return false;
  }

  @Override
  public ResultSet getUDTs(
      String catalog, String schemaPattern, String typeNamePattern, int[] types)
      throws SQLException {
    if (getUDTs == null) {
      connection.close();
      getUDTs =
          connection.prepareStatement(
              "select  null as TYPE_CAT, null as TYPE_SCHEM, "
                  + "null as TYPE_NAME,  null as CLASS_NAME,  null as DATA_TYPE, null as REMARKS, "
                  + "null as BASE_TYPE "
                  + "limit 0;");
    }

    getUDTs.clearParameters();
    return getUDTs.executeQuery();
  }

  @Override
  public Connection getConnection() {
    return connection;
  }

  @Override
  public boolean supportsSavepoints() {
    // TODO: return true when limbo supports save points
    return false;
  }

  @Override
  public boolean supportsNamedParameters() {
    // TODO: return true when both limbo and jdbc supports named parameters
    return false;
  }

  @Override
  public boolean supportsMultipleOpenResults() {
    return false;
  }

  @Override
  public boolean supportsGetGeneratedKeys() {
    // TODO: check
    return false;
  }

  @Override
  @SkipNullableCheck
  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
      throws SQLException {
    if (getSuperTypes == null) {
      connection.checkOpen();
      getSuperTypes =
          connection.prepareStatement(
              "select null as TYPE_CAT, null as TYPE_SCHEM, "
                  + "null as TYPE_NAME, null as SUPERTYPE_CAT, null as SUPERTYPE_SCHEM, "
                  + "null as SUPERTYPE_NAME limit 0;");
    }
    return getSuperTypes.executeQuery();
  }

  @Override
  @SkipNullableCheck
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    if (getSuperTables == null) {
      connection.checkOpen();
      getSuperTables =
          connection.prepareStatement(
              "select null as TABLE_CAT, null as TABLE_SCHEM, "
                  + "null as TABLE_NAME, null as SUPERTABLE_NAME limit 0;");
    }
    return getSuperTables.executeQuery();
  }

  @Override
  @SkipNullableCheck
  public ResultSet getAttributes(
      String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern)
      throws SQLException {
    if (getAttributes == null) {
      connection.checkOpen();
      getAttributes =
          connection.prepareStatement(
              "select null as TYPE_CAT, null as TYPE_SCHEM, "
                  + "null as TYPE_NAME, null as ATTR_NAME, null as DATA_TYPE, "
                  + "null as ATTR_TYPE_NAME, null as ATTR_SIZE, null as DECIMAL_DIGITS, "
                  + "null as NUM_PREC_RADIX, null as NULLABLE, null as REMARKS, null as ATTR_DEF, "
                  + "null as SQL_DATA_TYPE, null as SQL_DATETIME_SUB, null as CHAR_OCTET_LENGTH, "
                  + "null as ORDINAL_POSITION, null as IS_NULLABLE, null as SCOPE_CATALOG, "
                  + "null as SCOPE_SCHEMA, null as SCOPE_TABLE, null as SOURCE_DATA_TYPE limit 0;");
    }

    return getAttributes.executeQuery();
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability) {
    return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public int getResultSetHoldability() {
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public int getDatabaseMajorVersion() {
    // TODO - important
    return 0;
  }

  @Override
  public int getDatabaseMinorVersion() {
    // TODO - important
    return 0;
  }

  @Override
  public int getJDBCMajorVersion() {
    return 4;
  }

  @Override
  public int getJDBCMinorVersion() {
    return 2;
  }

  @Override
  public int getSQLStateType() {
    return DatabaseMetaData.sqlStateSQL99;
  }

  @Override
  public boolean locatorsUpdateCopy() {
    return false;
  }

  @Override
  public boolean supportsStatementPooling() {
    return false;
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    if (getSchemas == null) {
      connection.checkOpen();
      getSchemas =
          connection.prepareStatement("select null as TABLE_SCHEM, null as TABLE_CATALOG limit 0;");
    }

    return getSchemas.executeQuery();
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  @SkipNullableCheck
  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  @SkipNullableCheck
  public ResultSet getFunctionColumns(
      String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Not yet implemented by SQLite JDBC driver");
  }

  @Override
  @SkipNullableCheck
  public ResultSet getPseudoColumns(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  @SkipNullableCheck
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }
}
