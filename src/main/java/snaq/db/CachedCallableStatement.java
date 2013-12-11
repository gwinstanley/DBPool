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
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

/**
 * {@link CallableStatement} wrapper that provides caching support.
 *
 * @author Giles Winstanley
 */
public final class CachedCallableStatement extends CachedPreparedStatement implements CallableStatement
{
  /**
   * Creates a new {@link CachedCallableStatement} instance,
   * using the supplied {@link CallableStatement}.
   * @param sql SQL statement that may contain one or more '?' IN parameter placeholders
   * @param st {@code CallableStatement} instance to which database calls should be delegated
   */
  public CachedCallableStatement(String sql, CallableStatement st)
  {
    super(sql, st);
  }

  /**
   * Overridden to provide caching support.
   * @throws SQLException
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
  public String getString(int parameterIndex) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getString(parameterIndex);
  }

  @Override
  public boolean getBoolean(int parameterIndex) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getBoolean(parameterIndex);
  }

  @Override
  public byte getByte(int parameterIndex) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getByte(parameterIndex);
  }

  @Override
  public short getShort(int parameterIndex) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getShort(parameterIndex);
  }

  @Override
  public int getInt(int parameterIndex) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getInt(parameterIndex);
  }

  @Override
  public long getLong(int parameterIndex) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getLong(parameterIndex);
  }

  @Override
  public float getFloat(int parameterIndex) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getFloat(parameterIndex);
  }

  @Override
  public double getDouble(int parameterIndex) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getDouble(parameterIndex);
  }

  @Override
  public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getBigDecimal(parameterIndex, scale);
  }

  @Override
  public byte[] getBytes(int parameterIndex) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getBytes(parameterIndex);
  }

  @Override
  public Date getDate(int parameterIndex) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getDate(parameterIndex);
  }

  @Override
  public Time getTime(int parameterIndex) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getTime(parameterIndex);
  }

  @Override
  public Timestamp getTimestamp(int parameterIndex) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getTimestamp(parameterIndex);
  }

  @Override
  public Object getObject(int parameterIndex) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getObject(parameterIndex);
  }

  @Override
  public BigDecimal getBigDecimal(int parameterIndex) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getBigDecimal(parameterIndex);
  }

  @Override
  public Object getObject(int i, Map<String,Class<?>> map) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getObject(i, map);
  }

  @Override
  public Ref getRef(int i) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getRef(i);
  }

  @Override
  public Blob getBlob(int i) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getBlob(i);
  }

  @Override
  public Clob getClob(int i) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getClob(i);
  }

  @Override
  public Array getArray(int i) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getArray(i);
  }

  @Override
  public Date getDate(int parameterIndex, Calendar cal) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getDate(parameterIndex, cal);
  }

  @Override
  public Time getTime(int parameterIndex, Calendar cal) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getTime(parameterIndex, cal);
  }

  @Override
  public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getTimestamp(parameterIndex, cal);
  }

  @Override
  public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).registerOutParameter(parameterIndex, sqlType);
  }

  @Override
  public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).registerOutParameter(parameterIndex, sqlType, scale);
  }

  @Override
  public void registerOutParameter(int paramIndex, int sqlType, String typeName) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).registerOutParameter(paramIndex, sqlType, typeName);
  }

  @Override
  public boolean wasNull() throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).wasNull();
  }

  //**********************************
  // Interface methods from JDBC 3.0
  //**********************************

  @Override
  public void registerOutParameter(String parameterName, int sqlType) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).registerOutParameter(parameterName, sqlType);
  }

  @Override
  public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).registerOutParameter(parameterName, sqlType, scale);
  }

  @Override
  public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).registerOutParameter(parameterName, sqlType, typeName);
  }

  @Override
  public URL getURL(int parameterIndex) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getURL(parameterIndex);
  }

  @Override
  public void setURL(String parameterName, URL val) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setURL(parameterName, val);
  }

  @Override
  public void setNull(String parameterName, int sqlType) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setNull(parameterName, sqlType);
  }

  @Override
  public void setBoolean(String parameterName, boolean x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setBoolean(parameterName, x);
  }

  @Override
  public void setByte(String parameterName, byte x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setByte(parameterName, x);
  }

  @Override
  public void setShort(String parameterName, short x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setShort(parameterName, x);
  }

  @Override
  public void setDouble(String parameterName, double x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setDouble(parameterName, x);
  }

  @Override
  public void setFloat(String parameterName, float x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setFloat(parameterName, x);
  }

  @Override
  public void setInt(String parameterName, int x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setInt(parameterName, x);
  }

  @Override
  public void setLong(String parameterName, long x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setLong(parameterName, x);
  }

  @Override
  public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setBigDecimal(parameterName, x);
  }

  @Override
  public void setString(String parameterName, String x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setString(parameterName, x);
  }

  @Override
  public void setBytes(String parameterName, byte[] x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setBytes(parameterName, x);
  }

  @Override
  public void setDate(String parameterName, Date x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setDate(parameterName, x);
  }

  @Override
  public void setTime(String parameterName, Time x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setTime(parameterName, x);
  }

  @Override
  public void setTimestamp(String parameterName, Timestamp x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setTimestamp(parameterName, x);
  }

  @Override
  public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setAsciiStream(parameterName, x, length);
  }

  @Override
  public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setBinaryStream(parameterName, x, length);
  }

  @Override
  public void setObject(String parameterName, Object x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setObject(parameterName, x);
  }

  @Override
  public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setObject(parameterName, x, targetSqlType);
  }

  @Override
  public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setObject(parameterName, x, targetSqlType, scale);
  }

  @Override
  public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setCharacterStream(parameterName, reader, length);
  }

  @Override
  public void setDate(String parameterName, Date x, Calendar cal) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setDate(parameterName, x, cal);
  }

  @Override
  public void setTime(String parameterName, Time x, Calendar cal) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setTime(parameterName, x, cal);
  }

  @Override
  public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setTimestamp(parameterName, x, cal);
  }

  @Override
  public void setNull(String parameterName, int sqlType, String typeName) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setNull(parameterName, sqlType, typeName);
  }

  @Override
  public String getString(String parameterName) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getString(parameterName);
  }

  @Override
  public boolean getBoolean(String parameterName) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getBoolean(parameterName);
  }

  @Override
  public byte getByte(String parameterName) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getByte(parameterName);
  }

  @Override
  public short getShort(String parameterName) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getShort(parameterName);
  }

  @Override
  public int getInt(String parameterName) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getInt(parameterName);
  }

  @Override
  public long getLong(String parameterName) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getLong(parameterName);
  }

  @Override
  public float getFloat(String parameterName) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getFloat(parameterName);
  }

  @Override
  public double getDouble(String parameterName) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getDouble(parameterName);
  }

  @Override
  public byte[] getBytes(String parameterName) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getBytes(parameterName);
  }

  @Override
  public Date getDate(String parameterName) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getDate(parameterName);
  }

  @Override
  public Time getTime(String parameterName) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getTime(parameterName);
  }

  @Override
  public Timestamp getTimestamp(String parameterName) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getTimestamp(parameterName);
  }

  @Override
  public Object getObject(String parameterName) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getObject(parameterName);
  }

  @Override
  public BigDecimal getBigDecimal(String parameterName) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getBigDecimal(parameterName);
  }

  @Override
  public Object getObject(String parameterName, Map<String,Class<?>> map) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getObject(parameterName, map);
  }

  @Override
  public Ref getRef(String parameterName) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getRef(parameterName);
  }

  @Override
  public Blob getBlob(String parameterName) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getBlob(parameterName);
  }

  @Override
  public Clob getClob(String parameterName) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getClob(parameterName);
  }

  @Override
  public Array getArray(String parameterName) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getArray(parameterName);
  }

  @Override
  public Date getDate(String parameterName, Calendar cal) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getDate(parameterName, cal);
  }

  @Override
  public Time getTime(String parameterName, Calendar cal) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getTime(parameterName, cal);
  }

  @Override
  public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getTimestamp(parameterName, cal);
  }

  @Override
  public URL getURL(String parameterName) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getURL(parameterName);
  }

  //**********************************
  // Interface methods from JDBC 4.0
  //**********************************
  // --- JDBC 4.0 ---
  @Override
  public RowId getRowId(int parameterIndex) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getRowId(parameterIndex);
  }

  @Override
  public RowId getRowId(String parameterName) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getRowId(parameterName);
  }

  @Override
  public void setRowId(String parameterName, RowId x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setRowId(parameterName, x);
  }

  @Override
  public void setNString(String parameterName, String value) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setNString(parameterName, value);
  }

  @Override
  public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setNCharacterStream(parameterName, value, length);
  }

  @Override
  public void setNClob(String parameterName, NClob value) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setNClob(parameterName, value);
  }

  @Override
  public void setClob(String parameterName, Reader reader, long length) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setClob(parameterName, reader, length);
  }

  @Override
  public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setBlob(parameterName, inputStream, length);
  }

  @Override
  public void setNClob(String parameterName, Reader reader, long length) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setNClob(parameterName, reader, length);
  }

  @Override
  public NClob getNClob(int parameterIndex) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getNClob(parameterIndex);
  }

  @Override
  public NClob getNClob(String parameterName) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getNClob(parameterName);
  }

  @Override
  public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setSQLXML(parameterName, xmlObject);
  }

  @Override
  public SQLXML getSQLXML(int parameterIndex) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getSQLXML(parameterIndex);
  }

  @Override
  public SQLXML getSQLXML(String parameterName) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getSQLXML(parameterName);
  }

  @Override
  public String getNString(int parameterIndex) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getNString(parameterIndex);
  }

  @Override
  public String getNString(String parameterName) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getNString(parameterName);
  }

  @Override
  public Reader getNCharacterStream(int parameterIndex) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getNCharacterStream(parameterIndex);
  }

  @Override
  public Reader getNCharacterStream(String parameterName) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getNCharacterStream(parameterName);
  }

  @Override
  public Reader getCharacterStream(int parameterIndex) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getCharacterStream(parameterIndex);
  }

  @Override
  public Reader getCharacterStream(String parameterName) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getCharacterStream(parameterName);
  }

  @Override
  public void setBlob(String parameterName, Blob x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setBlob(parameterName, x);
  }

  @Override
  public void setClob(String parameterName, Clob x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setClob(parameterName, x);
  }

  @Override
  public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setAsciiStream(parameterName, x, length);
  }

  @Override
  public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setBinaryStream(parameterName, x, length);
  }

  @Override
  public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setCharacterStream(parameterName, reader, length);
  }

  @Override
  public void setAsciiStream(String parameterName, InputStream x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setAsciiStream(parameterName, x);
  }

  @Override
  public void setBinaryStream(String parameterName, InputStream x) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setBinaryStream(parameterName, x);
  }

  @Override
  public void setCharacterStream(String parameterName, Reader reader) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setCharacterStream(parameterName, reader);
  }

  @Override
  public void setNCharacterStream(String parameterName, Reader value) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setNCharacterStream(parameterName, value);
  }

  @Override
  public void setClob(String parameterName, Reader reader) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setClob(parameterName, reader);
  }

  @Override
  public void setBlob(String parameterName, InputStream inputStream) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setBlob(parameterName, inputStream);
  }

  @Override
  public void setNClob(String parameterName, Reader reader) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setNClob(parameterName, reader);
  }
  // --- End JDBC 4.0 ---

  //**********************************
  // Interface methods from JDBC 4.1
  //**********************************
  // --- JDBC 4.1 ---
  @Override
  public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getObject(parameterIndex, type);
  }

  @Override
  public <T> T getObject(String parameterName, Class<T> type) throws SQLException
  {
    if (isClosed())
      throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getObject(parameterName, type);
  }
  // --- End JDBC 4.1 ---
}
