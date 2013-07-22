/*
  ---------------------------------------------------------------------------
  DBPool : Java Database Connection Pooling <http://www.snaq.net/>
  Copyright (c) 2001-2013 Giles Winstanley. All Rights Reserved.

  This is file is part of the DBPool project, which is licenced under
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
import java.sql.*;
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

  public String getString(int parameterIndex) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getString(parameterIndex);
  }

  public boolean getBoolean(int parameterIndex) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getBoolean(parameterIndex);
  }

  public byte getByte(int parameterIndex) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getByte(parameterIndex);
  }

  public short getShort(int parameterIndex) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getShort(parameterIndex);
  }

  public int getInt(int parameterIndex) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getInt(parameterIndex);
  }

  public long getLong(int parameterIndex) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getLong(parameterIndex);
  }

  public float getFloat(int parameterIndex) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getFloat(parameterIndex);
  }

  public double getDouble(int parameterIndex) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getDouble(parameterIndex);
  }

  public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getBigDecimal(parameterIndex, scale);
  }

  public byte[] getBytes(int parameterIndex) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getBytes(parameterIndex);
  }

  public Date getDate(int parameterIndex) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getDate(parameterIndex);
  }

  public Time getTime(int parameterIndex) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getTime(parameterIndex);
  }

  public Timestamp getTimestamp(int parameterIndex) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getTimestamp(parameterIndex);
  }

  public Object getObject(int parameterIndex) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getObject(parameterIndex);
  }

  public BigDecimal getBigDecimal(int parameterIndex) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getBigDecimal(parameterIndex);
  }

  public Object getObject(int i, Map<String,Class<?>> map) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getObject(i, map);
  }

  public Ref getRef(int i) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getRef(i);
  }

  public Blob getBlob(int i) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getBlob(i);
  }

  public Clob getClob(int i) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getClob(i);
  }

  public Array getArray(int i) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getArray(i);
  }

  public Date getDate(int parameterIndex, Calendar cal) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getDate(parameterIndex, cal);
  }

  public Time getTime(int parameterIndex, Calendar cal) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getTime(parameterIndex, cal);
  }

  public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getTimestamp(parameterIndex, cal);
  }

  public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).registerOutParameter(parameterIndex, sqlType);
  }

  public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).registerOutParameter(parameterIndex, sqlType, scale);
  }

  public void registerOutParameter(int paramIndex, int sqlType, String typeName) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).registerOutParameter(paramIndex, sqlType, typeName);
  }

  public boolean wasNull() throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).wasNull();
  }

  //**********************************
  // Interface methods from JDBC 3.0
  //**********************************

  public void registerOutParameter(String parameterName, int sqlType) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).registerOutParameter(parameterName, sqlType);
  }

  public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).registerOutParameter(parameterName, sqlType, scale);
  }

  public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).registerOutParameter(parameterName, sqlType, typeName);
  }

  public URL getURL(int parameterIndex) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getURL(parameterIndex);
  }

  public void setURL(String parameterName, URL val) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setURL(parameterName, val);
  }

  public void setNull(String parameterName, int sqlType) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setNull(parameterName, sqlType);
  }

  public void setBoolean(String parameterName, boolean x) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setBoolean(parameterName, x);
  }

  public void setByte(String parameterName, byte x) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setByte(parameterName, x);
  }

  public void setShort(String parameterName, short x) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setShort(parameterName, x);
  }

  public void setDouble(String parameterName, double x) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setDouble(parameterName, x);
  }

  public void setFloat(String parameterName, float x) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setFloat(parameterName, x);
  }

  public void setInt(String parameterName, int x) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setInt(parameterName, x);
  }

  public void setLong(String parameterName, long x) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setLong(parameterName, x);
  }

  public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setBigDecimal(parameterName, x);
  }

  public void setString(String parameterName, String x) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setString(parameterName, x);
  }

  public void setBytes(String parameterName, byte[] x) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setBytes(parameterName, x);
  }

  public void setDate(String parameterName, Date x) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setDate(parameterName, x);
  }

  public void setTime(String parameterName, Time x) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setTime(parameterName, x);
  }

  public void setTimestamp(String parameterName, Timestamp x) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setTimestamp(parameterName, x);
  }

  public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setAsciiStream(parameterName, x, length);
  }

  public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setBinaryStream(parameterName, x, length);
  }

  public void setObject(String parameterName, Object x) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setObject(parameterName, x);
  }

  public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setObject(parameterName, x, targetSqlType);
  }

  public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setObject(parameterName, x, targetSqlType, scale);
  }

  public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setCharacterStream(parameterName, reader, length);
  }

  public void setDate(String parameterName, Date x, Calendar cal) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setDate(parameterName, x, cal);
  }

  public void setTime(String parameterName, Time x, Calendar cal) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setTime(parameterName, x, cal);
  }

  public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setTimestamp(parameterName, x, cal);
  }

  public void setNull(String parameterName, int sqlType, String typeName) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setNull(parameterName, sqlType, typeName);
  }

  public String getString(String parameterName) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getString(parameterName);
  }

  public boolean getBoolean(String parameterName) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getBoolean(parameterName);
  }

  public byte getByte(String parameterName) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getByte(parameterName);
  }

  public short getShort(String parameterName) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getShort(parameterName);
  }

  public int getInt(String parameterName) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getInt(parameterName);
  }

  public long getLong(String parameterName) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getLong(parameterName);
  }

  public float getFloat(String parameterName) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getFloat(parameterName);
  }

  public double getDouble(String parameterName) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getDouble(parameterName);
  }

  public byte[] getBytes(String parameterName) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getBytes(parameterName);
  }

  public Date getDate(String parameterName) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getDate(parameterName);
  }

  public Time getTime(String parameterName) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getTime(parameterName);
  }

  public Timestamp getTimestamp(String parameterName) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getTimestamp(parameterName);
  }

  public Object getObject(String parameterName) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getObject(parameterName);
  }

  public BigDecimal getBigDecimal(String parameterName) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getBigDecimal(parameterName);
  }

  public Object getObject(String parameterName, Map<String,Class<?>> map) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getObject(parameterName, map);
  }

  public Ref getRef(String parameterName) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getRef(parameterName);
  }

  public Blob getBlob(String parameterName) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getBlob(parameterName);
  }

  public Clob getClob(String parameterName) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getClob(parameterName);
  }

  public Array getArray(String parameterName) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getArray(parameterName);
  }

  public Date getDate(String parameterName, Calendar cal) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getDate(parameterName, cal);
  }

  public Time getTime(String parameterName, Calendar cal) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getTime(parameterName, cal);
  }

  public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getTimestamp(parameterName, cal);
  }

  public URL getURL(String parameterName) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getURL(parameterName);
  }

  //**********************************
  // Interface methods from JDBC 4.0
  //**********************************
  // --- JDBC 4.0 ---
  public RowId getRowId(int parameterIndex) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getRowId(parameterIndex);
  }

  public RowId getRowId(String parameterName) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getRowId(parameterName);
  }

  public void setRowId(String parameterName, RowId x) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setRowId(parameterName, x);
  }

  public void setNString(String parameterName, String value) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setNString(parameterName, value);
  }

  public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setNCharacterStream(parameterName, value, length);
  }

  public void setNClob(String parameterName, NClob value) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setNClob(parameterName, value);
  }

  public void setClob(String parameterName, Reader reader, long length) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setClob(parameterName, reader, length);
  }

  public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setBlob(parameterName, inputStream, length);
  }

  public void setNClob(String parameterName, Reader reader, long length) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setNClob(parameterName, reader, length);
  }

  public NClob getNClob(int parameterIndex) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getNClob(parameterIndex);
  }

  public NClob getNClob(String parameterName) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getNClob(parameterName);
  }

  public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setSQLXML(parameterName, xmlObject);
  }

  public SQLXML getSQLXML(int parameterIndex) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getSQLXML(parameterIndex);
  }

  public SQLXML getSQLXML(String parameterName) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getSQLXML(parameterName);
  }

  public String getNString(int parameterIndex) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getNString(parameterIndex);
  }

  public String getNString(String parameterName) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getNString(parameterName);
  }

  public Reader getNCharacterStream(int parameterIndex) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getNCharacterStream(parameterIndex);
  }

  public Reader getNCharacterStream(String parameterName) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getNCharacterStream(parameterName);
  }

  public Reader getCharacterStream(int parameterIndex) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getCharacterStream(parameterIndex);
  }

  public Reader getCharacterStream(String parameterName) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return ((CallableStatement)st).getCharacterStream(parameterName);
  }

  public void setBlob(String parameterName, Blob x) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setBlob(parameterName, x);
  }

  public void setClob(String parameterName, Clob x) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setClob(parameterName, x);
  }

  public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setAsciiStream(parameterName, x, length);
  }

  public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setBinaryStream(parameterName, x, length);
  }

  public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setCharacterStream(parameterName, reader, length);
  }

  public void setAsciiStream(String parameterName, InputStream x) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setAsciiStream(parameterName, x);
  }

  public void setBinaryStream(String parameterName, InputStream x) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setBinaryStream(parameterName, x);
  }

  public void setCharacterStream(String parameterName, Reader reader) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setCharacterStream(parameterName, reader);
  }

  public void setNCharacterStream(String parameterName, Reader value) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setNCharacterStream(parameterName, value);
  }

  public void setClob(String parameterName, Reader reader) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setClob(parameterName, reader);
  }

  public void setBlob(String parameterName, InputStream inputStream) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setBlob(parameterName, inputStream);
  }

  public void setNClob(String parameterName, Reader reader) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    ((CallableStatement)st).setNClob(parameterName, reader);
  }
  // --- End JDBC 4.0 ---
}
