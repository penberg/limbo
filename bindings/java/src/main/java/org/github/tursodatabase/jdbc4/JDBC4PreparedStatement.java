package org.github.tursodatabase.jdbc4;

import static java.util.Objects.requireNonNull;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import org.github.tursodatabase.annotations.SkipNullableCheck;
import org.github.tursodatabase.core.LimboResultSet;

public class JDBC4PreparedStatement extends JDBC4Statement implements PreparedStatement {

  private final String sql;

  public JDBC4PreparedStatement(JDBC4Connection connection, String sql) throws SQLException {
    super(connection);

    this.sql = sql;
    this.statement = connection.prepare(sql);
    this.statement.initializeColumnMetadata();
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    // TODO: check bindings etc
    requireNonNull(this.statement);
    return new JDBC4ResultSet(this.statement.getResultSet());
  }

  @Override
  public int executeUpdate() throws SQLException {
    requireNonNull(this.statement);
    final LimboResultSet resultSet = statement.getResultSet();
    resultSet.consumeAll();

    // TODO: return updated count
    return 0;
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    requireNonNull(this.statement);
    this.statement.bindNull(parameterIndex);
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    requireNonNull(this.statement);
    this.statement.bindInt(parameterIndex, x ? 1 : 0);
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    requireNonNull(this.statement);
    this.statement.bindInt(parameterIndex, x);
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    requireNonNull(this.statement);
    this.statement.bindInt(parameterIndex, x);
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    requireNonNull(this.statement);
    this.statement.bindInt(parameterIndex, x);
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    requireNonNull(this.statement);
    this.statement.bindLong(parameterIndex, x);
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    requireNonNull(this.statement);
    this.statement.bindDouble(parameterIndex, x);
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    requireNonNull(this.statement);
    this.statement.bindDouble(parameterIndex, x);
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    requireNonNull(this.statement);
    this.statement.bindText(parameterIndex, x.toString());
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    requireNonNull(this.statement);
    this.statement.bindText(parameterIndex, x);
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    requireNonNull(this.statement);
    this.statement.bindBlob(parameterIndex, x);
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException {
    // TODO
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException {
    // TODO
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    // TODO
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    // TODO
  }

  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    // TODO
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    // TODO
  }

  @Override
  public void clearParameters() throws SQLException {
    // TODO
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    // TODO
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    // TODO
  }

  @Override
  public boolean execute() throws SQLException {
    // TODO: check whether this is sufficient
    requireNonNull(this.statement);
    return statement.execute();
  }

  @Override
  public void addBatch() throws SQLException {
    // TODO
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length)
      throws SQLException {}

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {
    // TODO
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    // TODO
  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    // TODO
  }

  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException {
    // TODO
  }

  @Override
  @SkipNullableCheck
  public ResultSetMetaData getMetaData() throws SQLException {
    return null;
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    // TODO
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    // TODO
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    // TODO
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    // TODO
  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {
    // TODO
  }

  @Override
  @SkipNullableCheck
  public ParameterMetaData getParameterMetaData() throws SQLException {
    // TODO
    return null;
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    // TODO
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {
    // TODO
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length)
      throws SQLException {
    // TODO
  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    // TODO
  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    // TODO
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length)
      throws SQLException {
    // TODO
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    // TODO
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    // TODO
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
      throws SQLException {
    // TODO
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    // TODO
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    // TODO
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length)
      throws SQLException {
    // TODO
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    // TODO
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    // TODO
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    // TODO
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    // TODO
  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    // TODO
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    // TODO
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    // TODO
  }
}
