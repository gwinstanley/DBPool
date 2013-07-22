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

import java.sql.*;

/**
 * {@link Statement} wrapper that provides methods for caching support.
 *
 * @author Giles Winstanley
 */
public class CachedStatement implements Statement
{
  /** Exception message for trying to used a closed statement. */
  protected static final String MSG_STATEMENT_CLOSED = "Statement is closed";
  /** Assigned {@code StatementListener} instance. */
  private StatementListener listener;
  /** Delegate {@code Statement} instance. */
  protected Statement st;
  /** Flag indicating whether the connection is open. */
  protected boolean open = true;
  /** Flag indicating whether the connection is in the process of being closed. */
  protected boolean closing = false;
  /** Flag indicating a temporary &quot;checking&quot; status (used internally). */
  protected boolean checking = false;
  /** Flag indicating whether the statement can be cached. */
  protected boolean cacheable = false;

  /**
   * Creates a new {@link CachedStatement} instance, using the supplied {@link Statement}.
   * @param st {@code Statement} instance to which database calls should be delegated
   */
  public CachedStatement(Statement st)
  {
    this.st = st;
  }

  /**
   * Added to provide support for checking cached statement type.
   */
  void setChecking(boolean b)
  {
    checking = b;
  }

  /**
   * Sets whether this statement can be cached/recycled.
   */
  void setCacheable(boolean b)
  {
    cacheable = b;
  }

  /**
   * Returns whether this statement can be cached/recycled.
   */
  boolean isCacheable()
  {
    return cacheable;
  }

  /** Returns a string descriptions of the {@link ResultSet} parameters. */
  protected String getParametersString()
  {
    StringBuffer sb = new StringBuffer();
    try
    {
      switch(getResultSetType())
      {
        case ResultSet.TYPE_SCROLL_INSENSITIVE:
          sb.append("TYPE_SCROLL_INSENSITIVE");
          break;
        case ResultSet.TYPE_SCROLL_SENSITIVE:
          sb.append("TYPE_SCROLL_SENSITIVE");
          break;
        default:
          sb.append("TYPE_FORWARD_ONLY");
      }
    }
    catch (SQLException sqlx) { sb.append("TYPE_UNKNOWN"); }
    sb.append(',');
    try
    {
      switch(getResultSetConcurrency())
      {
        case ResultSet.CONCUR_UPDATABLE:
          sb.append("CONCUR_UPDATABLE");
          break;
        default:
          sb.append("CONCUR_READ_ONLY");
      }
    }
    catch (SQLException sqlx) { sb.append("CONCUR_UNKNOWN"); }
    sb.append(',');
    try
    {
      switch(getResultSetHoldability())
      {
        case ResultSet.CLOSE_CURSORS_AT_COMMIT:
          sb.append("CLOSE_CURSORS_AT_COMMIT");
          break;
        case ResultSet.HOLD_CURSORS_OVER_COMMIT:
          sb.append("HOLD_CURSORS_OVER_COMMIT");
      }
    }
    catch (SQLException sqlx) { sb.append("HOLD_UNKNOWN"); }
    return sb.toString();
  }

  // Cleans up the statement ready to be reused or closed.
  public void recycle() throws SQLException
  {
    ResultSet rs = st.getResultSet();
    if (rs != null)
      rs.close();

    try { st.clearWarnings(); }
    catch (SQLException sqlx) {}    // Caught to fix bug in some drivers.

    try { st.clearBatch(); }
    catch (SQLException sqlx) {}    // Caught to fix bug in some drivers.
  }

  /**
   * Overridden to provide caching support.
   */
  public void close() throws SQLException
  {
    if (!open)
      return;
    open = false;
    closing = true;
    // If listener registered, do callback, otherwise release statement.
    if (listener != null)
      listener.statementClosed(this);
    else
      release();
    closing = false;
  }

  /**
   * Overridden to provide caching support.
   */
  public void release() throws SQLException
  {
    st.close();
    st = null;  // Clear delegate to ensure no possible reuse.
    setStatementListener(null);
  }

  /**
   * Added to provide caching support.
   */
  void setOpen()
  {
    open = true;
  }

  /**
   * Added to provide caching support.
   */
  void setStatementListener(StatementListener x)
  {
    this.listener = x;
  }

  //**********************************
  // Interface methods from JDBC 2.0
  //**********************************

  public ResultSet executeQuery(String sql) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return st.executeQuery(sql);
  }

  public int executeUpdate(String sql) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return st.executeUpdate(sql);
  }

  public int getMaxFieldSize() throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return st.getMaxFieldSize();
  }

  public void setMaxFieldSize(int max) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    st.setMaxFieldSize(max);
  }

  public int getMaxRows() throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return st.getMaxRows();
  }

  public void setMaxRows(int max) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    st.setMaxRows(max);
  }

  public void setEscapeProcessing(boolean enable) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    st.setEscapeProcessing(enable);
  }

  public int getQueryTimeout() throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return st.getQueryTimeout();
  }

  public void setQueryTimeout(int seconds) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    st.setQueryTimeout(seconds);
  }

  public void cancel() throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    st.cancel();
  }

  public SQLWarning getWarnings() throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return st.getWarnings();
  }

  public void clearWarnings() throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    st.clearWarnings();
  }

  public void setCursorName(String name) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    st.setCursorName(name);
  }

  public boolean execute(String sql) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return st.execute(sql);
  }

  public ResultSet getResultSet() throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return st.getResultSet();
  }

  public int getUpdateCount() throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return st.getUpdateCount();
  }

  public boolean getMoreResults() throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return st.getMoreResults();
  }

  public void setFetchDirection(int direction) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    st.setFetchDirection(direction);
  }

  public int getFetchDirection() throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return st.getFetchDirection();
  }

  public void setFetchSize(int rows) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    st.setFetchSize(rows);
  }

  public int getFetchSize() throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return st.getFetchSize();
  }

  public int getResultSetConcurrency() throws SQLException
  {
    if (!open && !checking) throw new SQLException(MSG_STATEMENT_CLOSED);
    return st.getResultSetConcurrency();
  }

  public int getResultSetType() throws SQLException
  {
    if (!open && !checking) throw new SQLException(MSG_STATEMENT_CLOSED);
    return st.getResultSetType();
  }

  public void addBatch(String sql) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    st.addBatch(sql);
  }

  public void clearBatch() throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    st.clearBatch();
  }

  public int[] executeBatch() throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return st.executeBatch();
  }

  public Connection getConnection() throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return st.getConnection();
  }

  //**********************************
  // Interface methods from JDBC 3.0
  //**********************************

  public boolean getMoreResults(int current) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return st.getMoreResults(current);
  }

  public ResultSet getGeneratedKeys() throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return st.getGeneratedKeys();
  }

  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return st.executeUpdate(sql, autoGeneratedKeys);
  }

  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return st.executeUpdate(sql, columnIndexes);
  }

  public int executeUpdate(String sql, String[] columnNames) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return st.executeUpdate(sql, columnNames);
  }

  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return st.execute(sql, autoGeneratedKeys);
  }

  public boolean execute(String sql, int[] columnIndexes) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return st.execute(sql, columnIndexes);
  }

  public boolean execute(String sql, String[] columnNames) throws SQLException
  {
    if (!open) throw new SQLException(MSG_STATEMENT_CLOSED);
    return st.execute(sql, columnNames);
  }

  public int getResultSetHoldability() throws SQLException
  {
    if (!open && !checking) throw new SQLException(MSG_STATEMENT_CLOSED);
    return st.getResultSetHoldability();
  }

  //**********************************
  // Interface methods from JDBC 4.0
  //**********************************
  // --- JDBC 4.0 ---
  public boolean isWrapperFor(Class<?> iface) throws SQLException
  {
    return iface.isInstance(st);
  }

  public <T> T unwrap(Class<T> iface) throws SQLException
  {
    try
    {
      // Disallow recycling of this statement if unwrapped.
      T x = iface.cast(st);
      cacheable = false;
      return x;
    }
    catch (ClassCastException ccx)
    {
      SQLException sqlx = new SQLException("Invalid type specified for unwrap operation: " + iface.getName());
      sqlx.initCause(ccx);
      throw sqlx;
    }
  }

  public boolean isClosed() throws SQLException
  {
    return !open;
  }

  /**
   * Sets whether this statement should be considered poolable.
   * If a statement is already flagged as unpoolable it cannot be made poolable.
   * @param poolable flag indicating desired poolability
   * @throws SQLException if the request cannot be fulfilled
   */
  public void setPoolable(boolean poolable) throws SQLException
  {
    if (poolable && !cacheable)
      throw new SQLException("Cannot enable pooling on this statement");
    setCacheable(poolable);
  }

  /**
   * Returns whether this statement is poolable.
   * @throws SQLException
   */
  public boolean isPoolable() throws SQLException
  {
    return isCacheable();
  }
  // --- End JDBC 4.0 ---
}
