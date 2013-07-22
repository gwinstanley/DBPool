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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import snaq.util.Reusable;
import snaq.util.logging.LogUtil;

/**
 * {@link Connection} wrapper that implements statement caching.
 * 
 * @see snaq.db.CachedStatement
 * @see snaq.db.CachedPreparedStatement
 * @see snaq.db.CachedCallableStatement
 *
 * @author Giles Winstanley
 */
public final class CacheConnection implements Connection, StatementListener, Reusable
{
  /** Apache Commons Logging instance for writing log entries. */
  protected Log logger;
  /** Reference to pool's custom logging utility. */
  private LogUtil logUtil;
  /** Exception message for trying to used a closed connection. */
  private static final String MSG_CONNECTION_CLOSED = "Connection is closed";
  /** Default {@link ResultSet} type. */
  private static final int DEFAULT_RESULTSET_TYPE = ResultSet.TYPE_FORWARD_ONLY;
  /** Default {@link ResultSet} concurrency. */
  private static final int DEFAULT_RESULTSET_CONCURRENCY = ResultSet.CONCUR_READ_ONLY;
  /** Default {@link ResultSet} holdability. */
  private static final int DEFAULT_RESULTSET_HOLDABILITY = ResultSet.HOLD_CURSORS_OVER_COMMIT;
  /** Reference to the pool to which this connection belongs. */
  private final ConnectionPool pool;
  /** Reference to the raw delegate connection. */
  private final Connection con;
  /** Statement cache ({@code List} of {@code Statement}). */
  private final List<CachedStatement> ss = new ArrayList<CachedStatement>();
  /** Holder for Statement instances in use. */
  private final List<CachedStatement> ssUsed = new ArrayList<CachedStatement>();
  /** PreparedStatement cache ({@code Map} of {@code PreparedStatement}). */
  private final Map<String,List<CachedPreparedStatement>> ps = new HashMap<String,List<CachedPreparedStatement>>();
  /** Holder for {@code PreparedStatement} instances in use. */
  private final List<CachedPreparedStatement> psUsed = new ArrayList<CachedPreparedStatement>();
  /** CallableStatement cache ({@code Map} of {@code CallableStatement}). */
  private final Map<String,List<CachedCallableStatement>> cs = new HashMap<String,List<CachedCallableStatement>>();
  /** Holder for {@code CallableStatement} instances in use. */
  private final List<CachedCallableStatement> csUsed = new ArrayList<CachedCallableStatement>();
  /** Holder for non-cachable Statement instances that are in use. */
  private final List<CachedStatement> nonCachable = new ArrayList<CachedStatement>();
  /** Flag indicating whether {@link Statement} instances are to be cached. */
  private boolean cacheS;
  /** Flag indicating whether {@link Statement} instances are to be cached. */
  private boolean cacheP;
  /** Flag indicating whether {@link PreparedStatement} instances are to be cached. */
  private boolean cacheC;
  /** Count of number of requests for {@link Statement} instances. */
  private int ssReq;
  /** Count of number of cache hits for {@link Statement} instances. */
  private int ssHit;
  /** Count of number of requests for {@link PreparedStatement} instances. */
  private int psReq;
  /** Count of number of cache hits for {@link PreparedStatement} instances. */
  private int psHit;
  /** Count of number of requests for {@link CallableStatement} instances. */
  private int csReq;
  /** Count of number of cache hits for {@link CallableStatement} instances. */
  private int csHit;
  /** Flag indicating whether the connection is open. */
  private boolean open = true;
  /** Flag indicating whether the connection is in the process of being closed. */
  private boolean closing = false;
  /** Flag indicating whether the raw/delegate connection has been used. */
  private boolean usedDelegate = false;

  /**
   * Creates a new {@link CacheConnection} object, using the supplied {@link Connection}.
   * @param pool {@link ConnectionPool} to which this {@code CacheConnection} belongs
   * @param con {@link Connection} instance to which database calls should be delegated
   */
  public CacheConnection(ConnectionPool pool, Connection con)
  {
    this.pool = pool;
    this.con = con;
    setCacheAll(true);
    ssReq = ssHit = psReq = psHit = csReq = csHit = 0;
    // Send log output to same logger as the pool uses.
    logger = LogFactory.getLog(pool.getClass().getName() + "." + pool.getName());
    logUtil = pool.getCustomLogger();
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
  boolean isOpen()
  {
    return open;
  }

  /**
   * Sets whether to use caching for {@link Statement} instances.
   * @param cache whether to cache {@code Statement} instances
   */
  public void setCacheStatements(boolean cache)
  {
    // Release statements if required.
    if (cacheS && !cache)
    {
      try { flushSpareStatements(); }
      catch (SQLException sqlx) { log_warn(pool.getName() + ": " + sqlx.getMessage(), sqlx); }
    }
    this.cacheS = cache;
  }

  /**
   * Sets whether to use caching for {@link PreparedStatement} instances.
   * @param cache whether to cache {@code PreparedStatement} instances
   */
  public void setCachePreparedStatements(boolean cache)
  {
    // Release statements if required.
    if (cacheP && !cache)
    {
      try { flushSparePreparedStatements(); }
      catch (SQLException sqlx) { log_warn(pool.getName() + ": " + sqlx.getMessage(), sqlx); }
    }
    this.cacheP = cache;
  }

  /**
   * Sets whether to use caching for {@link CallableStatement} instances.
   * @param cache whether to cache {@code CallableStatement} instances
   */
  public void setCacheCallableStatements(boolean cache)
  {
    // Release statements if required.
    if (cacheC && !cache)
    {
      try { flushSpareCallableStatements(); }
      catch (SQLException sqlx) { log_warn(pool.getName() + ": " + sqlx.getMessage(), sqlx); }
    }
    this.cacheC = cache;
  }

  /**
   * Sets whether to use caching for all types of Statement.
   * @param cache whether to cache all types of statement
   */
  public void setCacheAll(boolean cache)
  {
    setCacheStatements(cache);
    setCachePreparedStatements(cache);
    setCacheCallableStatements(cache);
  }

  /**
   * Returns whether caching of all {@link Statement} instances is enabled.
   * @return true if all types of statements are flagged for caching, false otherwise.
   */
  public boolean isCachingAllStatements() { return cacheS && cacheP && cacheC; }

  /**
   * Returns whether caching of standard {@link Statement} instances is enabled.
   * @return true if {@code Statement} instances are flagged for caching, false otherwise.
   */
  public boolean isCachingStatements() { return cacheS; }

  /**
   * Returns whether caching of {@link PreparedStatement} instances is enabled.
   * @return true if {@code PreparedStatement} instances are flagged for caching, false otherwise.
   */
  public boolean isCachingPreparedStatements() { return cacheP; }

  /**
   * Returns whether caching of {@link CallableStatement} instances is enabled.
   * @return true if {@code CallableStatement} instances are flagged for caching, false otherwise.
   */
  public boolean isCachingCallableStatements() { return cacheC; }

  /**
   * Returns the delegate {@link Connection} instance for which this provides
   * a wrapper. This is provided as a convenience method for using database-specific
   * features for which the {@code Connection} object needs to be upcast.
   * (e.g. to use Oracle-specific features needs to be cast to
   * {@code oracle.jdbc.OracleConnection}).
   * <em>To maintain the stability of the pooling system it is important that the
   * raw connection is not destabilized when used in this way.</em>
   * @return the delegate {@code Connection} instance being used for database access.
   */
  public Connection getDelegateConnection()
  {
    usedDelegate = true;
    return con;
  }

  /**
   * Returns the delegate {@link Connection} instance for which this provides
   * a wrapper. This method is only for internal use, as it avoid flagging
   * external use.
   */
  Connection getRawConnection()
  {
    return con;
  }

  /**
   * Returns the {@code ConnectionPool} to which this {@code CacheConnection} belongs.
   * This is provided as a convenience for internal library code (testing etc.)
   */
  protected final ConnectionPool getPool()
  {
    return pool;
  }

  /** Logging relay method (to prefix pool name). */
  protected void log_warn(String s)
  {
    logger.warn(s);
    if (logUtil != null)
      logUtil.log(s);
  }

  /** Logging relay method (to prefix pool name). */
  protected void log_warn(String s, Throwable throwable)
  {
    logger.warn(s, throwable);
    if (logUtil != null)
      logUtil.log(s, throwable);
  }

  /** Logging relay method (to prefix pool name). */
  protected void log_debug(String s)
  {
    logger.debug(s);
    if (logUtil != null)
      logUtil.debug(s);
  }

  /** Logging relay method (to prefix pool name). */
  protected void log_debug(String s, Throwable throwable)
  {
    logger.debug(s, throwable);
    if (logUtil != null)
      logUtil.debug(s, throwable);
  }

  //******************************
  // Connection interface methods
  //******************************

  /** Overrides method to provide caching support. */
  public Statement createStatement() throws SQLException
  {
    return createStatement(DEFAULT_RESULTSET_TYPE, DEFAULT_RESULTSET_CONCURRENCY, DEFAULT_RESULTSET_HOLDABILITY);
  }

  /** Overrides method to provide caching support. */
  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException
  {
    return createStatement(resultSetType, resultSetConcurrency, DEFAULT_RESULTSET_HOLDABILITY);
  }

  /** Overrides method to provide caching support. */
  public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
  {
    if (!open)
      throw new SQLException(MSG_CONNECTION_CLOSED);

    CachedStatement cst = null;
    if (!cacheS)
    {
      cst = new CachedStatement(con.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability));
      cst.setCacheable(false);
      cst.setStatementListener(this);
      cst.setOpen();
    }
    else
    {
      synchronized(ss)
      {
        ssReq++;
        // Find Statement matching criteria required.
        for (Iterator it = ss.iterator(); it.hasNext();)
        {
          CachedStatement x = (CachedStatement)it.next();
          x.setChecking(true);
          if (x.getResultSetType() == resultSetType &&
                  x.getResultSetConcurrency() == resultSetConcurrency &&
                  x.getResultSetHoldability() == resultSetHoldability)
          {
            cst = x;
            it.remove();
            break;
          }
          x.setChecking(false);
        }
        // Prepare statement for user.
        if (cst != null)
        {
          cst.setOpen();
          ssHit++;
          log_debug(pool.getName() + ": Statement cache hit [" + cst.getParametersString() + "] - " + showHitRate(ssHit, ssReq, "S-"));
        }
        else
        {
          cst = new CachedStatement(con.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability));
          cst.setCacheable(true);
          cst.setStatementListener(this);
          cst.setOpen();
          log_debug(pool.getName() + ": Statement cache miss [" + cst.getParametersString() + "] - " + showHitRate(ssHit, ssReq, "S-"));
        }
      }
    }
    synchronized(ssUsed) { ssUsed.add(cst); }
    return cst;
  }

  /** Overrides method to provide caching support. */
  public PreparedStatement prepareStatement(String sql) throws SQLException
  {
    return prepareStatement(sql, DEFAULT_RESULTSET_TYPE, DEFAULT_RESULTSET_CONCURRENCY, DEFAULT_RESULTSET_HOLDABILITY);
  }

  /** Overrides method to provide caching support. */
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
  {
    return prepareStatement(sql, resultSetType, resultSetConcurrency, DEFAULT_RESULTSET_HOLDABILITY);
  }

  /**
   * Overrides method to provide caching support.
   */
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
  {
    if (!open)
      throw new SQLException(MSG_CONNECTION_CLOSED);

    CachedPreparedStatement cps = null;
    if (!cacheP)
    {
      cps = new CachedPreparedStatement(sql, con.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability));
      cps.setCacheable(false);
      cps.setStatementListener(this);
      cps.setOpen();
    }
    else
    {
      synchronized(ps)
      {
        psReq++;
        // Get List of cached PreparedStatements with matching SQL.
        List list = (List)ps.get(sql);
        if (list != null && !list.isEmpty())
        {
          // Find first free PreparedStatement with matching parameters.
          for (Iterator it = list.iterator(); it.hasNext();)
          {
            CachedPreparedStatement x = (CachedPreparedStatement)it.next();
            x.setChecking(true);
            if (x.getResultSetType() == resultSetType &&
                    x.getResultSetConcurrency() == resultSetConcurrency &&
                    x.getResultSetHoldability() == resultSetHoldability)
            {
              cps = x;
              it.remove();
              break;
            }
            x.setChecking(false);
          }
          // Remove cache mapping if list empty.
          if (list.isEmpty())
            ps.remove(sql);
        }
        // Prepare PreparedStatement for user.
        if (cps != null)
        {
          cps.setOpen();
          psHit++;
          log_debug(pool.getName() + ": PreparedStatement cache hit [" + sql + "," + cps.getParametersString() + "] - " + showHitRate(psHit, psReq, "PS-"));
        }
        else
        {
          cps = new CachedPreparedStatement(sql, con.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability));
          cps.setCacheable(true);
          cps.setStatementListener(this);
          cps.setOpen();
          log_debug(pool.getName() + ": PreparedStatement cache miss [" + sql + "," + cps.getParametersString() + "] - " + showHitRate(psHit, psReq, "PS-"));
        }
      }
    }
    synchronized(psUsed) { psUsed.add(cps); }
    return cps;
  }

  /** Overrides method to provide caching support. */
  public CallableStatement prepareCall(String sql) throws SQLException
  {
    return prepareCall(sql, DEFAULT_RESULTSET_TYPE, DEFAULT_RESULTSET_CONCURRENCY, DEFAULT_RESULTSET_HOLDABILITY);
  }

  /** Overrides method to provide caching support. */
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
  {
    return prepareCall(sql, resultSetType, resultSetConcurrency, DEFAULT_RESULTSET_HOLDABILITY);
  }

  /**
   * Overrides method to provide caching support.
   */
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
  {
    if (!open)
      throw new SQLException(MSG_CONNECTION_CLOSED);

    CachedCallableStatement ccs = null;
    if (!cacheC)
    {
      ccs = new CachedCallableStatement(sql, con.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability));
      ccs.setCacheable(false);
      ccs.setStatementListener(this);
      ccs.setOpen();
    }
    else
    {
      synchronized(cs)
      {
        csReq++;
        // Get List of cached CallableStatements with matching SQL.
        List list = (List)cs.get(sql);
        if (list != null && !list.isEmpty())
        {
          // Find first free CallableStatement with matching parameters.
          for (Iterator it = list.iterator(); it.hasNext();)
          {
            CachedCallableStatement x = (CachedCallableStatement)it.next();
            x.setChecking(true);
            if (x.getResultSetType() == resultSetType &&
                    x.getResultSetConcurrency() == resultSetConcurrency &&
                    x.getResultSetHoldability() == resultSetHoldability)
            {
              ccs = x;
              it.remove();
              break;
            }
            x.setChecking(false);
          }
          // Remove cache mapping if list empty.
          if (list.isEmpty())
            cs.remove(sql);
        }
        // Prepare CallableStatement for user.
        if (ccs != null)
        {
          ccs.setOpen();
          csHit++;
          log_debug(pool.getName() + ": CallableStatement cache hit [" + sql + "," + ccs.getParametersString() + "] - " + showHitRate(csHit, csReq, "CS-"));
        }
        else
        {
          CallableStatement st = con.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
          ccs = new CachedCallableStatement(sql, st);
          ccs.setCacheable(true);
          ccs.setStatementListener(this);
          ccs.setOpen();
          log_debug(pool.getName() + ": CallableStatement cache miss [" + sql + "," + ccs.getParametersString() + "] - " + showHitRate(csHit, csReq, "CS-"));
        }
      }
    }
    synchronized(csUsed) { csUsed.add(ccs); }
    return ccs;
  }

  /**
   * Callback invoked when a {@link CachedStatement} is closed.
   * This method should only be called by {@link CachedStatement} instances.
   */
  public void statementClosed(CachedStatement s) throws SQLException
  {
    // Check to see if statement is definitely non-cachable.
    synchronized(nonCachable)
    {
      if (nonCachable.remove(s))
      {
        s.release();
        return;
      }
    }
    // ...otherwise process as possibly cachable.
    if (s instanceof CachedCallableStatement)
    {
      synchronized(cs)
      {
        CachedCallableStatement ccs = (CachedCallableStatement)s;
        String key = ccs.getSQLString();
        synchronized(csUsed) { csUsed.remove(ccs); }
        // If caching disabled close statement.
        if (!cacheC || !ccs.isCacheable())
          ccs.release();
        else  // else try to recycle it.
        {
          try
          {
            ccs.recycle();
            // Place back in cache.
            List<CachedCallableStatement> list = cs.get(key);
            if (list == null)
            {
              list = new ArrayList<CachedCallableStatement>();
              cs.put(key, list);
            }
            list.add(ccs);
          }
          catch (SQLException sqlx)
          {
            ccs.release();
          }
        }
      }
    }
    else if (s instanceof CachedPreparedStatement)
    {
      synchronized(ps)
      {
        CachedPreparedStatement cps = (CachedPreparedStatement)s;
        String key = cps.getSQLString();
        synchronized(psUsed) { psUsed.remove(cps); }
        // If caching disabled close statement.
        if (!cacheP || !cps.isCacheable())
          cps.release();
        else  // else try to recycle it.
        {
          try
          {
            cps.recycle();
            // Place back in cache.
            List<CachedPreparedStatement> list = ps.get(key);
            if (list == null)
            {
              list = new ArrayList<CachedPreparedStatement>();
              ps.put(key, list);
            }
            list.add(cps);
          }
          catch (SQLException sqlx)
          {
            cps.release();
          }
        }
      }
    }
    else //if (s instanceof CachedStatement)
    {
      synchronized(ss)
      {
        synchronized(ssUsed) { ssUsed.remove(s); }
        // If caching disabled close statement.
        if (!cacheS || !s.isCacheable())
          s.release();
        else  // else try to recycle it.
        {
          try
          {
            s.recycle();
            ss.add(s);
          }
          catch (SQLException sqlx)
          {
            s.release();
          }
        }
      }
    }
  }

  // Calculate and shows a statement hit rate.
  private String showHitRate(int hits, int reqs, String prefix)
  {
    return (reqs == 0) ? "" : (prefix + "HitRate=" + (((float)hits / reqs) * 100f) + "%");
  }

  public String nativeSQL(String sql) throws SQLException
  {
    if (!open) throw new SQLException(MSG_CONNECTION_CLOSED);
    return con.nativeSQL(sql);
  }

  public void setAutoCommit(boolean autoCommit) throws SQLException
  {
    if (!open && !closing) throw new SQLException(MSG_CONNECTION_CLOSED);
    con.setAutoCommit(autoCommit);
  }

  public boolean getAutoCommit() throws SQLException
  {
    if (!open && !closing) throw new SQLException(MSG_CONNECTION_CLOSED);
    return con.getAutoCommit();
  }

  public void commit() throws SQLException
  {
    if (!open && !closing) throw new SQLException(MSG_CONNECTION_CLOSED);
    con.commit();
  }

  public void rollback() throws SQLException
  {
    if (!open && !closing) throw new SQLException(MSG_CONNECTION_CLOSED);
    con.rollback();
  }

  /**
   * Puts the connection back in a state where it can be reused.
   * This method should only be called by {@link ConnectionPool} instances.
   */
  public void recycle() throws SQLException
  {
    // Close all open Statements.
    if (cacheS)
    {
      int count = (ssUsed != null) ? ssUsed.size() : 0;
      if (count > 0)
      {
        log_debug(pool.getName() + ": Cleaning " + count + " cached Statement" + (count > 1 ? "s" : ""));
        synchronized(ssUsed)
        {
          while (!ssUsed.isEmpty())
            (ssUsed.remove(0)).close();
        }
      }
    }
    else
    {
      flushOpenStatements();
      flushSpareStatements();
    }

    // Close all open PreparedStatements.
    if (cacheP)
    {
      int count = (psUsed != null) ? psUsed.size() : 0;
      if (count > 0)
      {
        log_debug(pool.getName() + ": Cleaning " + count + " cached PreparedStatement" + (count > 1 ? "s" : ""));
        synchronized(psUsed)
        {
          while (!psUsed.isEmpty())
            (psUsed.remove(0)).close();
        }
      }
    }
    else
    {
      flushOpenPreparedStatements();
      flushSparePreparedStatements();
    }

    // Close all open CallableStatements.
    if (cacheC)
    {
      int count = (csUsed != null) ? csUsed.size() : 0;
      if (count > 0)
      {
        log_debug(pool.getName() + ": Cleaning " + count + " cached CallableStatement" + (count > 1 ? "s" : ""));
        synchronized(csUsed)
        {
          while (!csUsed.isEmpty())
            (csUsed.remove(0)).close();
        }
      }
    }
    else
    {
      flushOpenCallableStatements();
      flushSpareCallableStatements();
    }

    // Close all open non-cachable PreparedStatements.
    flushOpenNonCachableStatements();

    // If auto-commit is disabled, roll-back changes, and restore auto-commit.
    if (!getAutoCommit())
    {
      try
      {
        rollback();
      }
      catch (SQLException sqlx)
      {
        log_warn(pool.getName() + ": " + sqlx.getMessage(), sqlx);
      }
      setAutoCommit(true);
    }
    // Clear connection warnings.
    try
    {
      clearWarnings();
    }
    catch (SQLFeatureNotSupportedException sfnsx)
    {
    }

    // Clear type map entries.
    try
    {
      Map<String, Class<?>> tm = getTypeMap();
      if (tm != null)
        tm.clear();
    }
    catch (SQLFeatureNotSupportedException sfnsx)
    {
    }
  }

  /**
   * Determines if this object is &quot;dirty&quot; (i.e. unable to be recycled).
   */
  public boolean isDirty()
  {
    return usedDelegate && !pool.isRecycleAfterDelegateUse();
  }

  /**
   * Overrides method to provide caching support.
   */
  public void close() throws SQLException
  {
    if (!open)
      return;  // Uphold the descriptive contract of Connection interface.
    open = false;
    closing = true;
    // Hand connection (self) back to the pool for reuse.
    pool.freeConnection(this);
    closing = false;
  }

  /**
   * Returns the current number of spare Statements that are cached.
   */
  public int getSpareStatementCount()
  {
    return ss.size();
  }

  /**
   * Returns the current number of {@link Statement} instances that are in use
   * (not including {@link PreparedStatement} or {@link CallableStatement} instances).
   */
  public int getOpenStatementCount()
  {
    return ssUsed.size();
  }

  /**
   * Returns the current number of spare {@link PreparedStatement} instances that are cached.
   */
  public int getSparePreparedStatementCount()
  {
    int count = 0;
    synchronized(ps)
    {
      for (Iterator it = ps.values().iterator(); it.hasNext();)
        count += ((List)it.next()).size();
    }
    return count;
  }

  /**
   * Returns the current number of {@link PreparedStatement} instances that
   * are in use (not including {@link CallableStatement} instances).
   */
  public int getOpenPreparedStatementCount()
  {
    return psUsed.size();
  }

  /**
   * Returns the current number of spare {@link CallableStatement} instances that are cached.
   */
  public int getSpareCallableStatementCount()
  {
    int count = 0;
    synchronized(cs)
    {
      for (Iterator it = cs.values().iterator(); it.hasNext();)
        count += ((List)it.next()).size();
    }
    return count;
  }

  /**
   * Returns the current number of {@link CallableStatement} instances that are in use.
   */
  public int getOpenCallableStatementCount()
  {
    return csUsed.size();
  }

  /**
   * Returns the current number of non-cachable statements that are in use.
   * (Currently only some {@link PreparedStatement} instances are non-cachable
   * by virtue of a request made at creation for support for auto-generated keys.)
   * @see snaq.db.CacheConnection#prepareStatement(String, int)
   * @see snaq.db.CacheConnection#prepareStatement(String, int[])
   * @see snaq.db.CacheConnection#prepareStatement(String, String[])
   */
  public int getOpenNonCachableStatementCount()
  {
    synchronized (nonCachable) { return nonCachable.size(); }
  }

  /**
   * Flushes the spare {@link Statement} caches for this connection.
   */
  protected void flushSpareStatements() throws SQLException
  {
    synchronized(ss)
    {
      int count = ss.size();
      if (count > 0)
      {
        log_debug(pool.getName() + ": Closing " + count + " cached Statement" + (count > 1 ? "s" : ""));
        while (!ss.isEmpty())
          (ss.remove(0)).release();
      }
    }
  }

  /**
   * Flushes the open {@link Statement} cache for this connection.
   */
  protected void flushOpenStatements() throws SQLException
  {
    synchronized(ssUsed)
    {
      int count = ssUsed.size();
      if (count > 0)
      {
        log_debug(pool.getName() + ": Closing " + count + " open Statement" + (count > 1 ? "s" : ""));
        while (!ssUsed.isEmpty())
          (ssUsed.remove(0)).release();
      }
    }
  }

  /**
   * Flushes the spare {@link PreparedStatement} cache for this connection.
   */
  protected void flushSparePreparedStatements() throws SQLException
  {
    synchronized(ps)
    {
      int count = ps.size();
      if (count > 0)
      {
        log_debug(pool.getName() + ": Closing " + count + " cached PreparedStatement" + (count > 1 ? "s" : ""));
        for (List<CachedPreparedStatement> list : ps.values())
        {
          for (CachedPreparedStatement cps : list)
            cps.release();
        }
        ps.clear();
      }
    }
  }

  /**
   * Flushes the open {@link PreparedStatement} cache for this connection.
   */
  protected void flushOpenPreparedStatements() throws SQLException
  {
    synchronized(psUsed)
    {
      int count = psUsed.size();
      if (count > 0)
      {
        log_debug(pool.getName() + ": Closing " + count + " open PreparedStatement" + (count > 1 ? "s" : ""));
        while (!psUsed.isEmpty())
          (psUsed.remove(0)).release();
      }
    }
  }

  /**
   * Flushes the spare {@link CallableStatement} cache for this connection.
   */
  protected void flushSpareCallableStatements() throws SQLException
  {
    synchronized(cs)
    {
      int count = cs.size();
      if (count > 0)
      {
        log_debug(pool.getName() + ": Closing " + count + " cached CallableStatement" + (count > 1 ? "s" : ""));
        for (List<CachedCallableStatement> list : cs.values())
        {
          for (CachedCallableStatement ccs : list)
            ccs.release();
        }
        cs.clear();
      }
    }
  }

  /**
   * Flushes the open {@link CallableStatement} cache for this connection.
   */
  protected void flushOpenCallableStatements() throws SQLException
  {
    synchronized(csUsed)
    {
      int count = csUsed.size();
      if (count > 0)
      {
        log_debug(pool.getName() + ": Closing " + count + " open CallableStatement" + (count > 1 ? "s" : ""));
        while (!csUsed.isEmpty())
          (csUsed.remove(0)).release();
      }
    }
  }

  /**
   * Flushes the non-cachable {@link Statement} instances for this connection.
   */
  protected void flushOpenNonCachableStatements() throws SQLException
  {
    synchronized(nonCachable)
    {
      int count = nonCachable.size();
      if (count > 0)
      {
        log_debug(pool.getName() + ": Closing " + count + " open non-cachable Statement" + (count > 1 ? "s" : ""));
        while (!nonCachable.isEmpty())
        {
          try { ((Statement)nonCachable.remove(0)).close(); }
          catch (SQLException sqlx) { logger.warn(pool.getName() + ": " + sqlx.getMessage(), sqlx); }
        }
      }
    }
  }

  /**
   * Destroys the wrapped {@link Connection} instance.
   * This method should only be called by {@link ConnectionPool} instances.
   */
  public void release() throws SQLException
  {
    open = false;
    List<SQLException> list = new ArrayList<SQLException>();

    try
    {
      flushSpareStatements();
      flushOpenStatements();
    }
    catch (SQLException e)
    {
      list.add(e);
    }

    try
    {
      flushSparePreparedStatements();
      flushOpenPreparedStatements();
    }
    catch (SQLException e)
    {
      list.add(e);
    }

    try
    {
      flushSpareCallableStatements();
      flushOpenCallableStatements();
    }
    catch (SQLException e)
    {
      list.add(e);
    }

    try
    {
      flushOpenNonCachableStatements();
    }
    catch (SQLException e)
    {
      list.add(e);
    }

    try
    {
      con.close();
    }
    catch (SQLException e)
    {
      list.add(e);
    }

    if (!list.isEmpty())
    {
      SQLException sqle = new SQLException("Problem releasing connection resources");
      for (SQLException x : list)
      {
        sqle.setNextException(x);
        sqle = x;
      }
      throw sqle;
    }
  }

  public boolean isClosed() throws SQLException
  {
    return !open;
  }

  public DatabaseMetaData getMetaData() throws SQLException
  {
    if (!open) throw new SQLException(MSG_CONNECTION_CLOSED);
    return con.getMetaData();
  }

  public void setReadOnly(boolean readOnly) throws SQLException
  {
    if (!open) throw new SQLException(MSG_CONNECTION_CLOSED);
    con.setReadOnly(readOnly);
  }

  public boolean isReadOnly() throws SQLException
  {
    if (!open) throw new SQLException(MSG_CONNECTION_CLOSED);
    return con.isReadOnly();
  }

  public void setCatalog(String catalog) throws SQLException
  {
    if (!open) throw new SQLException(MSG_CONNECTION_CLOSED);
    con.setCatalog(catalog);
  }

  public String getCatalog() throws SQLException
  {
    if (!open) throw new SQLException(MSG_CONNECTION_CLOSED);
    return con.getCatalog();
  }

  public void setTransactionIsolation(int level) throws SQLException
  {
    if (!open) throw new SQLException(MSG_CONNECTION_CLOSED);
    con.setTransactionIsolation(level);
  }

  public int getTransactionIsolation() throws SQLException
  {
    if (!open) throw new SQLException(MSG_CONNECTION_CLOSED);
    return con.getTransactionIsolation();
  }

  public SQLWarning getWarnings() throws SQLException
  {
    if (!open) throw new SQLException(MSG_CONNECTION_CLOSED);
    return con.getWarnings();
  }

  public void clearWarnings() throws SQLException
  {
    if (!open && !closing) throw new SQLException(MSG_CONNECTION_CLOSED);
    con.clearWarnings();
  }

  public Map<String,Class<?>> getTypeMap() throws SQLException
  {
    if (!open && !closing) throw new SQLException(MSG_CONNECTION_CLOSED);
    return con.getTypeMap();
  }

  public void setTypeMap(Map<String,Class<?>> map) throws SQLException
  {
    if (!open) throw new SQLException(MSG_CONNECTION_CLOSED);
    con.setTypeMap(map);
  }

  //**********************************
  // Interface methods from JDBC 3.0
  //**********************************

  public void setHoldability(int holdability) throws SQLException
  {
    if (!open) throw new SQLException(MSG_CONNECTION_CLOSED);
    con.setHoldability(holdability);
  }

  public int getHoldability() throws SQLException
  {
    if (!open) throw new SQLException(MSG_CONNECTION_CLOSED);
    return con.getHoldability();
  }

  public Savepoint setSavepoint() throws SQLException
  {
    if (!open) throw new SQLException(MSG_CONNECTION_CLOSED);
    return con.setSavepoint();
  }

  public Savepoint setSavepoint(String name) throws SQLException
  {
    if (!open) throw new SQLException(MSG_CONNECTION_CLOSED);
    return con.setSavepoint(name);
  }

  public void rollback(Savepoint savepoint) throws SQLException
  {
    if (!open && !closing) throw new SQLException(MSG_CONNECTION_CLOSED);
    con.rollback(savepoint);
  }

  public void releaseSavepoint(Savepoint savepoint) throws SQLException
  {
    if (!open) throw new SQLException(MSG_CONNECTION_CLOSED);
    con.releaseSavepoint(savepoint);
  }

  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException
  {
    if (!open) throw new SQLException(MSG_CONNECTION_CLOSED);
    CachedPreparedStatement x = new CachedPreparedStatement(sql, con.prepareStatement(sql, autoGeneratedKeys));
    x.setCacheable(false);
    x.setStatementListener(this);
    synchronized(nonCachable) { nonCachable.add(x); }
    return x;
  }

  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException
  {
    if (!open) throw new SQLException(MSG_CONNECTION_CLOSED);
    CachedPreparedStatement x = new CachedPreparedStatement(sql, con.prepareStatement(sql, columnIndexes));
    x.setCacheable(false);
    x.setStatementListener(this);
    synchronized(nonCachable) { nonCachable.add(x); }
    return x;
  }

  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException
  {
    if (!open) throw new SQLException(MSG_CONNECTION_CLOSED);
    CachedPreparedStatement x = new CachedPreparedStatement(sql, con.prepareStatement(sql, columnNames));
    x.setCacheable(false);
    x.setStatementListener(this);
    synchronized(nonCachable) { nonCachable.add(x); }
    return x;
  }

  public Connection getConnection() throws SQLException
  {
    return this;
  }

  //**********************************
  // Interface methods from JDBC 4.0
  //**********************************
  // --- JDBC 4.0 ---
  public boolean isWrapperFor(Class<?> iface) throws SQLException
  {
    return iface.isInstance(con);
  }

  public <T> T unwrap(Class<T> iface) throws SQLException
  {
    try
    {
      // Flag as direct use of delegate.
      T x = iface.cast(con);
      usedDelegate = true;
      return x;
    }
    catch (ClassCastException ccx)
    {
      SQLException sqlx = new SQLException("Invalid type specified for unwrap operation: " + iface.getName());
      sqlx.initCause(ccx);
      throw sqlx;
    }
  }

  public Clob createClob() throws SQLException
  {
    if (!open) throw new SQLException(MSG_CONNECTION_CLOSED);
    return con.createClob();
  }

  public Blob createBlob() throws SQLException
  {
    if (!open) throw new SQLException(MSG_CONNECTION_CLOSED);
    return con.createBlob();
  }

  public NClob createNClob() throws SQLException
  {
    if (!open) throw new SQLException(MSG_CONNECTION_CLOSED);
    return con.createNClob();
  }

  public SQLXML createSQLXML() throws SQLException
  {
    if (!open) throw new SQLException(MSG_CONNECTION_CLOSED);
    return con.createSQLXML();
  }

  public boolean isValid(int timeout) throws SQLException
  {
    if (!open) throw new SQLException(MSG_CONNECTION_CLOSED);
    return con.isValid(timeout);
  }

  public void setClientInfo(String name, String value) throws SQLClientInfoException
  {
//    if (!open) throw new SQLException(MSG_CONNECTION_CLOSED);
    con.setClientInfo(name, value);
  }

  public void setClientInfo(Properties properties) throws SQLClientInfoException
  {
//    if (!open) throw new SQLException(MSG_CONNECTION_CLOSED);
    con.setClientInfo(properties);
  }

  public String getClientInfo(String name) throws SQLException
  {
    if (!open) throw new SQLException(MSG_CONNECTION_CLOSED);
    return con.getClientInfo(name);
  }

  public Properties getClientInfo() throws SQLException
  {
    if (!open) throw new SQLException(MSG_CONNECTION_CLOSED);
    return con.getClientInfo();
  }

  public Array createArrayOf(String typeName, Object[] elements) throws SQLException
  {
    if (!open) throw new SQLException(MSG_CONNECTION_CLOSED);
    return con.createArrayOf(typeName, elements);
  }

  public Struct createStruct(String typeName, Object[] attributes) throws SQLException
  {
    if (!open) throw new SQLException(MSG_CONNECTION_CLOSED);
    return con.createStruct(typeName, attributes);
  }
  // --- End JDBC 4.0 ---
}
