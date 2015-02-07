/*
  ---------------------------------------------------------------------------
  DBPool : Java Database Connection Pooling <http://www.snaq.net/>
  Copyright (c) 2001-2013 Giles Winstanley. All Rights Reserved.

  This is file is part of the DBPool project, which is licensed under
  the BSD-style licence terms shown below.
  ---------------------------------------------------------------------------
  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions are met:

  1. Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.

  2. Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

  3. The name of the author may not be used to endorse or promote products
  derived from this software without specific prior written permission.

  4. Redistributions of modified versions of the source code, must be
  accompanied by documentation detailing which parts of the code are not part
  of the original software.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDER "AS IS" AND ANY EXPRESS OR
  IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
  OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
  IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
  OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
  WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
  OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
  ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
  ---------------------------------------------------------------------------
 */
package snaq.db;

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
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

/**
 * {@link PreparedStatement} wrapper that provides caching support.
 *
 * @author Giles Winstanley
 */
public class CachedPreparedStatement extends CachedStatement implements PreparedStatement
{
  protected String sql;

  /**
   * Creates a new {@link CachedPreparedStatement} object,
   * using the supplied {@link PreparedStatement}.
   * @param sql SQL statement that may contain one or more '?' IN parameter placeholders
   * @param st {@code PreparedStatement} instance to which database calls should be delegated
   */
  public CachedPreparedStatement(String sql, PreparedStatement st)
  {
    super(st);
    this.sql = sql;
  }

  String getSQLString()
  {
    return sql;
  }

  String toTemplateString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append(super.toString());
    sb.append(" [");
    sb.append(sql);
    sb.append(',');
    sb.append(getParametersString());
    sb.append("]");
    return sb.toString();
  }

  /**
   * Overridden to add PreparedStatement specific code.
   * @throws SQLException if thrown while attempting to recycle statement
   */
  @Override
  public void recycle() throws SQLException
  {
    super.recycle();
    PreparedStatement ps = (PreparedStatement)st;

    try
    {
      ps.clearParameters();
    }
    catch (NullPointerException npx)  // Catch clearParameters() bug in Java when no parameters.
    {
    }
    catch (SQLException sqlx)  // Caught to fix bug in some drivers.
    {
      sqlx.printStackTrace();
    }
  }

  /**
   * Overridden to provide caching support.
   * @throws SQLException if thrown while attempting to release statement
   */
  @Override
  public void release() throws SQLException
  {
    st.close();
    st = null;  // Clear delegate to ensure no possible reuse.
    setStatementListener(null);
  }

  //**********************************
  // Interface methods from JDBC 2.0
  //**********************************

  @Override
  public ResultSet executeQuery() throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((PreparedStatement)st).executeQuery();
  }

  @Override
  public int executeUpdate() throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((PreparedStatement)st).executeUpdate();
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setNull(parameterIndex, sqlType);
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setBoolean(parameterIndex, x);
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setByte(parameterIndex, x);
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setShort(parameterIndex, x);
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setInt(parameterIndex, x);
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setLong(parameterIndex, x);
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setFloat(parameterIndex, x);
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setDouble(parameterIndex, x);
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setBigDecimal(parameterIndex, x);
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setString(parameterIndex, x);
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setBytes(parameterIndex, x);
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setDate(parameterIndex, x);
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setTime(parameterIndex, x);
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setTimestamp(parameterIndex, x);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setAsciiStream(parameterIndex, x, length);
  }

  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setUnicodeStream(parameterIndex, x, length);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setBinaryStream(parameterIndex, x, length);
  }

  @Override
  public void clearParameters() throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).clearParameters();
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scale) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setObject(parameterIndex, x, targetSqlType, scale);
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setObject(parameterIndex, x, targetSqlType);
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setObject(parameterIndex, x);
  }

  @Override
  public boolean execute() throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((PreparedStatement)st).execute();
  }

  @Override
  public void addBatch() throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).addBatch();
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setCharacterStream(parameterIndex, reader, length);
  }

  @Override
  public void setRef(int i, Ref x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setRef(i, x);
  }

  @Override
  public void setBlob(int i, Blob x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setBlob(i, x);
  }

  @Override
  public void setClob(int i, Clob x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setClob(i, x);
  }

  @Override
  public void setArray(int i, Array x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setArray(i, x);
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((PreparedStatement)st).getMetaData();
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setDate(parameterIndex, x, cal);
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setTime(parameterIndex, x, cal);
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setTimestamp(parameterIndex, x, cal);
  }

  @Override
  public void setNull(int paramIndex, int sqlType, String typeName) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setNull(paramIndex, sqlType, typeName);
  }

  //**********************************
  // Interface methods from JDBC 3.0
  //**********************************

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((PreparedStatement)st).getParameterMetaData();
  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setURL(parameterIndex, x);
  }

  //**********************************
  // Interface methods from JDBC 4.0
  //**********************************
  // --- JDBC 4.0 ---
  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setRowId(parameterIndex, x);
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setNString(parameterIndex, value);
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setNCharacterStream(parameterIndex, value, length);
  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setNClob(parameterIndex, value);
  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setClob(parameterIndex, reader, length);
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setBlob(parameterIndex, inputStream, length);
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setNClob(parameterIndex, reader, length);
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setSQLXML(parameterIndex, xmlObject);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setAsciiStream(parameterIndex, x, length);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setBinaryStream(parameterIndex, x, length);
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setCharacterStream(parameterIndex, reader, length);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setAsciiStream(parameterIndex, x);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setBinaryStream(parameterIndex, x);
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setCharacterStream(parameterIndex, reader);
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setNCharacterStream(parameterIndex, value);
  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setClob(parameterIndex, reader);
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setBlob(parameterIndex, inputStream);
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setNClob(parameterIndex, reader);
  }
  // --- End JDBC 4.0 ---

  //**********************************
  // Interface methods from JDBC 4.2
  //**********************************
  // --- JDBC 4.2 ---
  @Override
  public long executeLargeUpdate() throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((PreparedStatement)st).executeLargeUpdate();
  }

  @Override
  public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setObject(parameterIndex, x, targetSqlType);
  }

  @Override
  public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((PreparedStatement)st).setObject(parameterIndex, x, targetSqlType, scaleOrLength);
  }
  // --- End JDBC 4.2 ---
}
