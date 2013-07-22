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

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A {@link DataSource} implementation which produces {@link Connection}
 * instances. This implementation internally references a {@link ConnectionPool}
 * which it uses to provide transparent connection pooling to the client.
 *
 * @author Giles Winstanley
 */
public class DBPoolDataSource implements DataSource, ConnectionPoolListener
{
  /** Name prefix to use for {@code ConnectionPool} instance (JNDI name is appended). */
  protected static final String POOL_NAME_PREFIX = "DBPoolDataSource-";
  /** Apache Commons Logging shared instance for writing log entries. */
  protected static final Log logger = LogFactory.getLog(DBPoolDataSource.class);
  /** {@code Driver} to use for database access. */
  protected transient Driver driver;
  /** {@code ConnectionPool} instance used to source connections. */
  protected transient ConnectionPool pool;
  /** {@code PrintWriter} instance used for logging. */
  protected transient PrintWriter logWriter;
  // -------------------------------
  // Standard DataSource properties.
  // -------------------------------
//  private String dataSourceName;
  /** Description of this DataSource. */
  private String description;
//  private String databaseName;
//  private String networkProtocol;
  /** Username for accessing the database. */
  private String user;
  /** Password for accessing the database. */
  private String password;
//  private int portNumber;
//  private String roleName;
//  private String serverName;
  // -----------------------------
  // Custom DataSource properties.
  // -----------------------------
  /** JNDI name of the {@code DBPoolDataSource}. */
  private String name;
  /** Fully-qualified class name of the JDBC Driver for database access. */
  private String driverClassName;
  /** JDBC URI protocol string. */
  private String url;
  /** Fully-qualified class name of the {@link PasswordDecoder} for decoding passwords. */
  private String passwordDecoderClassName;
  /** Fully-qualified class name of the {@link ConnectionValidator} for validating database connections. */
  private String validatorClassName;
  /** SQL query string to use for validating database connections. */
  private String validationQuery;
  /** Connection pool {@code minPool} parameter. */
  private int minPool = 0;
  /** Connection pool {@code maxPool} parameter. */
  private int maxPool = 0;
  /** Connection pool {@code maxSize} parameter. */
  private int maxSize = 0;
  /** Connection pool {@code idleTimeout} parameter (seconds). */
  private int idleTimeout = 0;
  /** Timeout in seconds for database connection attempts. */
  private int loginTimeout = 3;
  /** Flag detemining whether a pool shutdown-hook is registered. */
  private boolean shutdownHook = false;

  /**
   * Creates a new {@code DBPoolDataSource} instance.
   */
  public DBPoolDataSource()
  {
  }

  /**
   * Registers a shutdown hook for this ConnectionPoolManager instance
   * to ensure it is released if the JVM exits
   */
  public synchronized void registerShutdownHook()
  {
    this.shutdownHook = true;
    if (pool != null)
      pool.registerShutdownHook();
  }

  /**
   * Deregisters a registered shutdown hook for this ConnectionPoolManager instance.
   */
  public synchronized void removeShutdownHook()
  {
    this.shutdownHook = false;
    if (pool != null)
      pool.removeShutdownHook();
  }

  /**
   * Writes a message to the log.
   */
  protected synchronized void log(String message)
  {
    String s = (name != null) ? (name + ": " + message) : message;
    logger.info(s);
    if (logWriter != null)
      logWriter.println(s);
  }

  /**
   * Writes a message with a {@code Throwable} to the log file.
   */
  protected synchronized void log(String message, Throwable throwable)
  {
    String s = (name != null) ? (name + ": " + message) : message;
    logger.info(s, throwable);
    if (logWriter != null)
    {
      logWriter.println(s);
      throwable.printStackTrace(logWriter);
    }
  }

  /**
   * Creates a new {@code DBPoolDataSource} instance.
   * @throws java.sql.SQLException if required resources cannot be located/established
   */
  protected synchronized void createConnectionPool() throws SQLException
  {
    logger.debug("ClassLoader is of type: " + getClass().getClassLoader().getClass().getName());

    // Registered JDBC driver (if required).
    if (driver == null)
    {
      try
      {
        driver = (Driver)Class.forName(getDriverClassName()).newInstance();
        DriverManager.registerDriver(driver);
      }
      catch (Exception ex)
      {
        SQLException sqlx = new SQLException("Unable to register JDBC driver: " + getDriverClassName());
        sqlx.initCause(ex);
        log(sqlx.getMessage(), sqlx);
        throw sqlx;
      }
    }
    // Create connection pool.
    String poolName = POOL_NAME_PREFIX + name;
    pool = new ConnectionPool(poolName, getMinPool(), getMaxPool(), getMaxSize(), getIdleTimeout(), getUrl(), getUser(), getPassword());
    pool.addConnectionPoolListener(this);
    if (getLogWriter() != null)
      pool.setLog(getLogWriter());
    if (shutdownHook)
      pool.registerShutdownHook();

    // Set ConnectionValidator as required.
    if (validatorClassName != null && !validatorClassName.equals(""))
    {
      try
      {
        ConnectionValidator cv = (ConnectionValidator)Class.forName(validatorClassName).newInstance();
        pool.setValidator(cv);
      }
      catch (Exception ex)
      {
        log("Unable to instantiate validator class: " + validatorClassName);
      }
    }
    else if (validationQuery != null && !validationQuery.equals(""))
    {
      ConnectionValidator cv = new SimpleQueryValidator(validationQuery);
      pool.setValidator(cv);
    }

    // Set PasswordDecoder as required.
    if (passwordDecoderClassName != null && !passwordDecoderClassName.equals(""))
    {
      try
      {
        PasswordDecoder pd = (PasswordDecoder)Class.forName(passwordDecoderClassName).newInstance();
        pool.setPasswordDecoder(pd);
      }
      catch (Exception ex)
      {
        log("Unable to instantiate password decoder class: " + passwordDecoderClassName);
      }
    }
  }

  /**
   * Attempts to establish a connection with the database.
   * @return a {@code Connection} instance, or null if unable to connect
   * @throws java.sql.SQLException if a database access error occurs
   */
  public synchronized Connection getConnection() throws SQLException
  {
    if (pool == null)
      createConnectionPool();
    // Get connection from pool.
    return getLoginTimeout() > 0 ? pool.getConnection(1000L * getLoginTimeout()) : pool.getConnection();
  }

  /**
   * Attempts to establish a connection with the database.
   * @param username the database user on whose behalf the connection is being made
   * @param password the user's password
   * @return a {@code Connection} instance, or null if unable to connect
   * @throws java.sql.SQLException if a database access error occurs
   */
  public synchronized Connection getConnection(String username, String password) throws SQLException
  {
    throw new UnsupportedOperationException("Unsupport method; use getConnection() instead.");
  }

  /**
   * Releases (cleans up resources of) the internal {@code ConnectionPool}
   * instance. This method should be called to close all the pooled connections.
   */
  public synchronized void releaseConnectionPool()
  {
    if (pool != null)
      pool.releaseForcibly();
    pool = null;
  }

  /**
   * Returns the description of this DataSource.
   */
  public synchronized String getDescription()
  {
    return this.description;
  }

  /**
   * Sets the description of this DataSource.
   */
  public synchronized void setDescription(String description)
  {
    this.description = description;
  }

  /**
   * Returns the username to use with this DataSource.
   */
  public synchronized String getUser()
  {
    return this.user;
  }

  /**
   * Alias for {@link #getUser()}.
   * @deprecated Use {@link #getUser()} method instead.
   */
  @Deprecated
  public String getUsername() { return getUser(); }

  /**
   * Sets the username to use with this DataSource.
   */
  public synchronized void setUser(String user)
  {
    if (pool != null)
      throw new UnsupportedOperationException("Cannot call this method after DBPoolDataSource has been initialized");
    this.user = user;
  }

  /**
   * Alias for {@link #setUser(String)}.
   * @deprecated Use {@link #setUser(String)} method instead.
   */
  @Deprecated
  public void setUsername(String username) { setUser(username); }

  /**
   * Returns the password to use with this DataSource.
   */
  public synchronized String getPassword()
  {
    return this.password;
  }

  /**
   * Sets the password to use with this DataSource.
   */
  public synchronized void setPassword(String password)
  {
    if (pool != null)
      throw new UnsupportedOperationException("Cannot call this method after DBPoolDataSource has been initialized");
    this.password = password;
  }

  /**
   * Returns the JNDI name of the {@code DBPoolDataSource} instance.
   */
  public synchronized String getName()
  {
    return name;
  }

  /**
   * Sets the JNDI name of the {@code DBPoolDataSource} instance.
   */
  public synchronized void setName(String name)
  {
    this.name = name;
  }

  /**
   * Returns the fully-qualified class name for the JDBC driver to use.
   */
  public synchronized String getDriverClassName()
  {
    return this.driverClassName;
  }

  /**
   * Sets the fully-qualified class name for the JDBC driver to use.
   */
  public synchronized void setDriverClassName(String driverClassName)
  {
    if (pool != null)
      throw new UnsupportedOperationException("Cannot call this method after DBPoolDataSource has been initialized");
    this.driverClassName = driverClassName;
  }

  /**
   * Returns the class name of the {@link PasswordDecoder} to use with this DataSource.
   */
  public synchronized String getPasswordDecoderClassName()
  {
    return this.passwordDecoderClassName;
  }

  /**
   * Sets the class name of the {@link PasswordDecoder} to use with this
   * DataSource. The specified class should implement the
   * {@code PasswordDecoder} interface, and have a no-argument constructor
   * which can be used to instantiate it for use.
   */
  public synchronized void setPasswordDecoderClassName(String decoderClassName)
  {
    if (pool != null)
      throw new UnsupportedOperationException("Cannot call this method after DBPoolDataSource has been initialized");
    this.passwordDecoderClassName = decoderClassName;
  }

  /**
   * Returns the class name of the {@link ConnectionValidator} to use with this DataSource.
   */
  public synchronized String getValidatorClassName()
  {
    return this.validatorClassName;
  }

  /**
   * Sets the class name of the {@link ConnectionValidator} to use with this
   * DataSource. The specified class should implement the
   * {@code ConnectionValidator} interface, and have a no-argument constructor
   * which can be used to instantiate it for use.
   */
  public synchronized void setValidatorClassName(String validatorClassName)
  {
    if (pool != null)
      throw new UnsupportedOperationException("Cannot call this method after DBPoolDataSource has been initialized");
    this.validatorClassName = validatorClassName;
  }

  /**
   * Returns the SQL query string to issue for connection validation.
   * This query is only used if the {@code validationClassName} has not
   * been explicitly set, in which case this query string is used with an
   * instance of {@link SimpleQueryValidator}.
   */
  public synchronized String getValidationQuery()
  {
    return this.validationQuery;
  }

  /**
   * Sets the SQL query string to issue for connection validation.
   * This query is only used if the {@code validationClassName} has not
   * been explicitly set, in which case this query string is used with an
   * instance of {@link SimpleQueryValidator}.
   */
  public synchronized void setValidationQuery(String validationQuery)
  {
    if (pool != null)
      throw new UnsupportedOperationException("Cannot call this method after DBPoolDataSource has been initialized");
    this.validationQuery = validationQuery;
  }

  /**
   * Returns the JDBC URL to use with this DataSource.
   */
  public synchronized String getUrl()
  {
    return this.url;
  }

  /**
   * Sets the JDBC URL to use with this DataSource.
   */
  public synchronized void setUrl(String url)
  {
    if (pool != null)
      throw new UnsupportedOperationException("Cannot call this method after DBPoolDataSource has been initialized");
    this.url = url;
  }

  /**
   * Returns the minimum number of pooled connections in the underlying {@link ConnectionPool}.
   */
  public synchronized int getMinPool()
  {
    return minPool;
  }

  /**
   * Sets the minimum number of pooled connections in the underlying {@link ConnectionPool}.
   */
  public synchronized void setMinPool(int minPool)
  {
    this.maxPool = minPool;
    if (pool != null)
      pool.setParameters(this.minPool, pool.getMaxPool(), pool.getMaxSize(), pool.getIdleTimeout());
  }

  /**
   * Returns the maximum number of pooled connections in the underlying {@link ConnectionPool}.
   */
  public synchronized int getMaxPool()
  {
    return maxPool;
  }

  /**
   * Sets the maximum number of pooled connections in the underlying {@link ConnectionPool}.
   */
  public synchronized void setMaxPool(int maxPool)
  {
    this.maxPool = maxPool;
    if (pool != null)
      pool.setParameters(pool.getMinPool(), this.maxPool, pool.getMaxSize(), pool.getIdleTimeout());
  }

  /**
   * Returns the maximum number of connections in the underlying {@link ConnectionPool}.
   */
  public synchronized int getMaxSize()
  {
    return maxSize;
  }

  /**
   * Sets the maximum number of connections in the underlying {@link ConnectionPool}.
   */
  public synchronized void setMaxSize(int maxSize)
  {
    this.maxSize = maxSize;
    if (pool != null)
      pool.setParameters(pool.getMinPool(), pool.getMaxPool(), this.maxSize, pool.getIdleTimeout());
  }

  /**
   * Returns the idle timeout (seconds) for connections in the underlying {@link ConnectionPool}.
   */
  public synchronized int getIdleTimeout()
  {
    return idleTimeout;
  }

  /**
   * Sets the idle timeout (seconds) for connections in the underlying {@link ConnectionPool}.
   */
  public synchronized void setIdleTimeout(int idleTimeout)
  {
    this.idleTimeout = idleTimeout;
    if (pool != null)
      pool.setParameters(pool.getMinPool(), pool.getMaxPool(), pool.getMaxSize(), this.idleTimeout * 1000L);
  }

  /**
   * Returns the idle timeout (seconds) for connections in the underlying {@link ConnectionPool}.
   * @deprecated Use {@link #getIdleTimeout()} instead.
   */
  @Deprecated
  public int getExpiryTime() { return getIdleTimeout(); }

  /**
   * Sets the idle timeout (seconds) for connections in the underlying {@link ConnectionPool}.
   * @deprecated Use {@link #setIdleTimeout(int)} instead.
   */
  @Deprecated
  public void setExpiryTime(int idleTimeout) { setIdleTimeout(idleTimeout); }

  /**
   * Retrieves the log writer for this DataSource.
   */
  public synchronized PrintWriter getLogWriter()
  {
    return logWriter;
  }

  /**
   * Sets the log writer for this DataSource to the given {@code PrintWriter}.
   * @param out the new log writer; to disable logging, set to null
   */
  public synchronized void setLogWriter(PrintWriter out)
  {
    this.logWriter = out;
    if (pool != null)
      pool.setLog(this.logWriter);
  }

  /**
   * Sets the maximum time in seconds that this DataSource will wait while
   * attempting to connect to a database. A value of zero specifies that the
   * timeout is the default system timeout if there is one;
   * otherwise, it specifies that there is no timeout.
   * When a {@code DataSource} object is created, the login timeout is
   * initially zero.
   * @param seconds the DataSource login time limit
   */
  public synchronized void setLoginTimeout(int seconds)
  {
    this.loginTimeout = seconds;
  }

  public synchronized int getLoginTimeout()
  {
    return this.loginTimeout;
  }

  /**
   * Releases the delegate {@link ConnectionPool} instance.
   */
  public void release() { pool.release(); }

  /**
   * Asynchronously releases the delegate {@link ConnectionPool} instance.
   */
  public void releaseAsync() { pool.releaseAsync(); }

  /**
   * Forcibly releases the delegate {@link ConnectionPool} instance.
   */
  public void releaseForcibly() { pool.releaseForcibly(); }

  @Override
  public synchronized String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getName());
    sb.append('[');
    sb.append("name=");
    sb.append(getName());
    sb.append(",driverClassName=");
    sb.append(getDriverClassName());
    sb.append(",url=");
    sb.append(getUrl());
    sb.append(",user=");
    sb.append(getUser());
    sb.append(",loginTimeout=");
    sb.append(getLoginTimeout());
    sb.append(",minPool=");
    sb.append(getMinPool());
    sb.append(",maxPool=");
    sb.append(getMaxPool());
    sb.append(",maxSize=");
    sb.append(getMaxSize());
    sb.append(",idleTimeout=");
    sb.append(getIdleTimeout());
    sb.append("s");
    return sb.toString();
  }

  public synchronized boolean isWrapperFor(Class<?> iface) throws SQLException
  {
    return iface.isInstance(pool);
  }

  public synchronized <T> T unwrap(Class<T> iface) throws SQLException
  {
    try
    {
      return iface.cast(pool);
    }
    catch (ClassCastException ccx)
    {
      SQLException sqlx = new SQLException("Invalid interface specified for unwrap operation: " + iface.getName());
      sqlx.initCause(ccx);
      throw sqlx;
    }
  }

  //*************************************************************
  // Event listener methods to maintain synchronization with pool
  //*************************************************************

  public void poolInitCompleted(ConnectionPoolEvent evt) {}
  public void poolCheckOut(ConnectionPoolEvent evt) {}
  public void poolCheckIn(ConnectionPoolEvent evt) {}
  public void validationError(ConnectionPoolEvent evt) {}
  public void maxPoolLimitReached(ConnectionPoolEvent evt) {}
  public void maxPoolLimitExceeded(ConnectionPoolEvent evt) {}
  public void maxSizeLimitReached(ConnectionPoolEvent evt) {}
  public void maxSizeLimitError(ConnectionPoolEvent evt) {}
  public void poolFlushed(ConnectionPoolEvent evt) {}

  // Synchronizes parameters in case they are changed externally.
  public synchronized void poolParametersChanged(ConnectionPoolEvent evt)
  {
    synchronized(pool)
    {
      this.minPool = pool.getMinPool();
      this.maxPool = pool.getMaxPool();
      this.maxSize = pool.getMaxSize();
      this.idleTimeout = (int)pool.getIdleTimeout();
    }
  }

  public synchronized void poolReleased(ConnectionPoolEvent evt)
  {
    pool.removeConnectionPoolListener(this);
    pool = null;
  }
}
