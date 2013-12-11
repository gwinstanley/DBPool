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

import java.lang.management.ManagementFactory;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import snaq.util.JmxUtils;
import snaq.util.EventDispatcher;
import snaq.util.EventNotifier;
import snaq.util.ObjectPool;
import snaq.util.ObjectPoolEvent;
import snaq.util.ObjectPoolListener;

/**
 * Implementation of a database connection pool.
 *
 * @see snaq.db.CacheConnection
 * @see snaq.db.CachedCallableStatement
 * @see snaq.db.CachedPreparedStatement
 * @author Giles Winstanley
 */
public class ConnectionPool extends ObjectPool<CacheConnection>
{
  /** JDBC URL to use for connecting to the database server. */
  private String url;
  /** Username to use for connecting to the database server. */
  private String user;
  /** Password to use for connecting to the database server. */
  private String pass;
  /** Properties for this pool. */
  private Properties props;
  /** {@link ConnectionValidator} instance for this pool. */
  private ConnectionValidator validator = new DefaultValidator();
  /** {@link PasswordDecoder} instance for this pool. */
  private PasswordDecoder decoder;
  /** Flag determining whether {@link Statement} instances are cached. */
  private boolean cacheSS;
  /** Flag determining whether {@link PreparedStatement} instances are cached. */
  private boolean cachePS;
  /** Flag determining whether {@link CallableStatement} instances are cached. */
  private boolean cacheCS;
  /** List to hold listeners for {@link ConnectionPoolEvent} events. */
  private final List<ConnectionPoolListener> listeners = new CopyOnWriteArrayList<>();
  /** Event dispatcher thread instance to issue events in a thread-safe manner. */
  private EventDispatcher<ConnectionPoolListener,ConnectionPoolEvent> eventDispatcher;
  /** Flag indicating whether to recycle connections after their raw/delegate connection has been used. */
  private boolean recycleAfterDelegateUse = false;
  /** Flag indicating whether this pool has had an MBean registered for it. */
  private boolean mbeanRegistered = false;
  /** Registered MBean name for this pool with the MBean server. */
  private String mbeanRegisteredName;

  /**
   * Creates a new {@code ConnectionPool} instance.
   * @param name pool name
   * @param minPool minimum number of pooled connections, or 0 for none
   * @param maxPool maximum number of pooled connections, or 0 for none
   * @param maxSize maximum number of possible connections, or 0 for no limit
   * @param idleTimeout idle timeout (seconds) for idle pooled connections, or 0 for no timeout
   * @param url JDBC connection URL
   * @param username database username
   * @param password password for the database username supplied
   */
  public ConnectionPool(String name, int minPool, int maxPool, int maxSize, long idleTimeout, String url, String username, String password)
  {
    super(name, minPool, maxPool, maxSize, idleTimeout);
    this.url = url;
    this.user = username;
    this.pass = password;
    this.props = null;
    setCaching(true);
    addObjectPoolListener(new EventRelay<>());
  }

  /**
   * Creates a new {@code ConnectionPool} instance (with {@code minPool=0}).
   * @param name pool name
   * @param maxPool maximum number of pooled connections, or 0 for none
   * @param maxSize maximum number of possible connections, or 0 for no limit
   * @param idleTimeout idle timeout (seconds) for idle pooled connections, or 0 for no timeout
   * @param url JDBC connection URL
   * @param username database username
   * @param password password for the database username supplied
   */
  public ConnectionPool(String name, int maxPool, int maxSize, long idleTimeout, String url, String username, String password)
  {
    this(name, 0, maxPool, maxSize, idleTimeout, url, username, password);
  }

  /**
   * Creates a new {@code ConnectionPool} instance.
   * @param name pool name
   * @param minPool minimum number of pooled connections, or 0 for none
   * @param maxPool maximum number of pooled connections, or 0 for none
   * @param maxSize maximum number of possible connections, or 0 for no limit
   * @param idleTimeout idle timeout (seconds) for idle pooled connections, or 0 for no timeout
   * @param url JDBC connection URL
   * @param props connection properties
   */
  public ConnectionPool(String name, int minPool, int maxPool, int maxSize, long idleTimeout, String url, Properties props)
  {
    this(name, minPool, maxPool, maxSize, idleTimeout, url, null, null);
    this.props = props;
    // Extract password to allow for re-injection as decoded version.
    if (props != null)
      this.pass = props.getProperty("password");
  }

  /**
   * Creates a new {@code ConnectionPool} instance (with {@code minPool=0}).
   * @param name pool name
   * @param maxPool maximum number of pooled connections, or 0 for none
   * @param maxSize maximum number of possible connections, or 0 for no limit
   * @param idleTimeout idle timeout (seconds) for idle pooled connections, or 0 for no timeout
   * @param url JDBC connection URL
   * @param props connection properties
   */
  public ConnectionPool(String name, int maxPool, int maxSize, long idleTimeout, String url, Properties props)
  {
    this(name, 0, maxPool, maxSize, idleTimeout, url, props);
  }

  /**
   * Registers this pool with the platform MBean server.
   * Once this method has been called, each pool instance is accessible as an
   * MBean using the {@code javax.management} API.
   */
  public void registerMBean()
  {
    if (mbeanRegistered)
      return;
    try
    {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      this.mbeanRegisteredName = "snaq.db:type=ConnectionPool,name=\"" + getName() + "\"";
      ObjectName name = new ObjectName(mbeanRegisteredName);
      mbs.registerMBean(JmxUtils.createObjectPoolMBean(this), name);
      mbeanRegistered = true;
      log_info("Registered MBean for JMX access");
    }
    catch (Exception ex)
    {
      log_warn("Unable to register pool with MBean server", ex);
    }
  }

  /**
   * Removes this pool from the platform MBean server registration list.
   * Once this method has been called, each pool instance is accessible as an
   * MBean using the {@code javax.management} API.
   */
  public void unregisterMBean()
  {
    if (!mbeanRegistered)
    {
      log_warn("Unable to unregister pool from MBean server: not registered");
      return;
    }
    try
    {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName name = new ObjectName(mbeanRegisteredName);
      mbs.unregisterMBean(name);
      mbeanRegistered = false;
    }
    catch (MalformedObjectNameException | InstanceNotFoundException | MBeanRegistrationException ex)
    {
      log_warn("Unable to unregister pool from MBean server", ex);
    }
  }

  /**
   * Creates a new {@link Connection} object.
   * @return A new CacheConnection instance
   * @throws SQLException
   */
  @Override
  protected CacheConnection create() throws SQLException
  {
    Connection con = null;
    CacheConnection ccon = null;
    try
    {
      // Properties instance specified, so take details from there.
      if (props != null)
      {
        // If password coded, temporarily set decoded version in properties.
        if (decoder != null)
          props.setProperty("password", new String(decoder.decode(pass)));
        log_info("Getting connection (properties): " + url);
        con = DriverManager.getConnection(url, props);
        // Clear decoded password (minor security benefit).
        if (decoder != null)
          props.setProperty("password", pass);
      }
      // No Properties specified, so use username/password specified.
      else if (user != null)
      {
        if (decoder != null)
        {
          log_info("Getting connection (user/enc.password): " + url);
          con = DriverManager.getConnection(url, user, new String(decoder.decode(pass)));
        }
        else
        {
          log_info("Getting connection (user/password): " + url);
          con = DriverManager.getConnection(url, user, pass);
        }
      }
      // No username specified - try with just the URL.
      else
      {
        log_info("Getting connection (just URL): " + url);
        con = DriverManager.getConnection(url);
      }

      // Add caching wrapper to connection.
      ccon = new CacheConnection(this, con);
      ccon.setCacheStatements(cacheSS);
      ccon.setCachePreparedStatements(cachePS);
      ccon.setCacheCallableStatements(cacheCS);
      log_info("Created a new connection");

      // Check for warnings.
      SQLWarning warn = con.getWarnings();
      while (warn != null)
      {
        log_info("Warning - " + warn.getMessage());
        warn = warn.getNextWarning();
      }
    }
    catch (SQLException sqlx)
    {
      log_info("Can't create a new connection for " + url, sqlx);
      // Clean up open connection.
      try { if (con != null) con.close(); }
      catch (SQLException sqlx2) { log_warn("Unable to close connection", sqlx2); }
      // Rethrow exception.
      throw sqlx;
    }
    return ccon;
  }

  /**
   * Validates a {@link CacheConnection} object.
   * @param cc connection to validate
   * @return true if cc is valid, false otherwise
   */
  @Override
  protected boolean isValid(final CacheConnection cc)
  {
    if (cc == null)
      return false;
    if (validator == null)
      return true;

    try
    {
      boolean valid = validator.isValid(cc.getRawConnection());
      if (!valid)
        firePoolEvent(ConnectionPoolEvent.VALIDATION_ERROR);
      return valid;
    }
    catch (SQLException sqlx)
    {
      log_debug("SQLException during validation", sqlx);
      return false;
    }
  }

  /**
   * Sets the validator class for {@link Connection} instances.
   * @param cv ConnectionValidator instance to use for this pool
   */
  public void setValidator(ConnectionValidator cv)
  {
    validator = cv;
  }

  /**
   * Returns the current {@link ConnectionValidator} class.
   * @return The current {@link ConnectionValidator} class
   */
  public ConnectionValidator getValidator()
  {
    return validator;
  }

  /**
   * Sets the {@link PasswordDecoder} class.
   * @param pd PasswordDecoder instance to use for this pool
   */
  public void setPasswordDecoder(PasswordDecoder pd)
  {
    decoder = pd;
  }

  /**
   * Returns the current {@link PasswordDecoder} class.
   * @return The current {@link PasswordDecoder} class
   */
  public PasswordDecoder getPasswordDecoder()
  {
    return decoder;
  }

  /**
   * Closes the specified {@link CacheConnection} object.
   * @param cc connection to destroy
   */
  @Override
  protected void destroy(final CacheConnection cc)
  {
    if (cc == null)
      return;
    try
    {
      cc.release();
      log_info("Destroyed connection");
    }
    catch (SQLException sqlx)
    {
      log_warn("Can't destroy connection", sqlx);
    }
  }

  /**
   * Gets a {@link Connection} from the pool.
   * @return a connection from the pool, or {@code null} if nothing available
   * @throws SQLException if there is an error creating a new connection
   */
  public Connection getConnection() throws SQLException
  {
    try
    {
      CacheConnection cc = super.checkOut();
      if (cc != null)
      {
        cc.setOpen();
        return cc;
      }
      return null;
    }
    catch (Exception ex)
    {
      log_warn("Error getting connection", ex);
      if (ex instanceof SQLException)
        throw (SQLException)ex;
      else
      {
        Throwable t = ex.getCause();
        while (t != null)
        {
          log_warn("Error getting connection", ex);
          t = t.getCause();
        }
        throw new SQLException(ex.getMessage());
      }
    }
  }

  /**
   * Gets a {@link Connection} from the pool, waiting a maximum of
   * {@code timeout} milliseconds for one to become available,
   * returning {@code null} if not.
   * @param timeout timeout value in milliseconds
   * @return item from the pool, or {@code null} if nothing available within timeout period
   * @throws SQLException if there is an error getting a connection
   */
  public Connection getConnection(long timeout) throws SQLException
  {
    if (timeout < 0)
      throw new IllegalArgumentException("Invalid timeout value specified: " + timeout);
    try
    {
      CacheConnection cc = super.checkOut(timeout);
      if (cc != null)
      {
        cc.setOpen();
        return cc;
      }
      return null;
    }
    catch (Exception ex)
    {
      if (ex instanceof SQLException)
        throw (SQLException)ex;
      else
      {
        log_warn("Error getting connection", ex);
        throw new SQLException(ex.getMessage());
      }
    }
  }

  /**
   * Returns a {@link Connection} to the pool (for internal use only).
   * Connections obtained from the pool should be returned by calling
   * {@link Connection#close()}.
   * @param c connection to free back to the pool
   * @throws SQLException
   */
  protected void freeConnection(Connection c) throws SQLException
  {
    if (c == null || !CacheConnection.class.isInstance(c))
      log_warn("Attempt to return invalid item");
    else
      super.checkIn((CacheConnection)c);
  }

  @Override
  protected void preRelease()
  {
    if (mbeanRegistered)
      unregisterMBean();
  }

  @Override
  protected void postRelease()
  {
    // Destroy event dispatch thread.
    listeners.clear();
    if (eventDispatcher != null)
    {
      eventDispatcher.halt();
      try { eventDispatcher.join(); }
      catch (InterruptedException ix) { log_warn("Interrupted during halting of event dispatch thread", ix); }
      eventDispatcher = null;
    }
  }

  @Override
  protected float getIdleTimeoutMultiplier()
  {
    return 1000f;
  }

  /**
   * Determines whether to perform statement caching.
   * This applies to all types of statements (normal, prepared, callable).
   * @param b Whether to perform statement caching
   */
  public final void setCaching(boolean b)
  {
    cacheSS = cachePS = cacheCS = b;
  }

  /**
   * Determines whether to perform statement caching.
   * @param ss whether to cache {@link Statement} objects
   * @param ps whether to cache {@link PreparedStatement} objects
   * @param cs whether to cache {@link CallableStatement} objects
   */
  public void setCaching(boolean ss, boolean ps, boolean cs)
  {
    cacheSS = ss;
    cachePS = ps;
    cacheCS = cs;
  }

  /**
   * Returns whether the pool caches {@code Statement} instances for each connection.
   * @return true if pool is caching Statement instances, false otherwise
   */
  public boolean isCachingStatements()
  {
    return cacheSS;
  }

  /**
   * Returns whether the pool caches {@code PreparedStatement} instances for each connection.
   * @return true if pool is caching PreparedStatement instances, false otherwise
   */
  public boolean isCachingPreparedStatements()
  {
    return cachePS;
  }

  /**
   * Returns whether the pool caches {@code CallableStatement} instances for each connection.
   * @return true if pool is caching CallableStatement instances, false otherwise
   */
  public boolean isCachingCallableStatements()
  {
    return cacheCS;
  }

  /**
   * Sets whether the connection may be recycled if the underlying
   * raw/delegate connection has been used (default: false).
   * <P>Each {@code CacheConnection} instance tracks whether a call to
   * {@link CacheConnection#getDelegateConnection()} has been made, and by default
   * prevents recycling of the connection if so, in order to help maintain
   * integrity of the pool. In certain circumstances it may be beneficial in
   * terms of performance to enable such recycling, provided the raw connections
   * are not compromised in any way, and the {@link CacheConnection#close()}
   * method is called on each {@code CacheConnection} instance and NOT the
   * raw connection.
   * @param b whether connections are recycled after use of underlying delegate connection
   */
  public final void setRecycleAfterDelegateUse(boolean b)
  {
    recycleAfterDelegateUse = b;
  }

  /**
   * Returns whether connections may be recycled if the underlying
   * raw/delegate connection has been used.
   * @return true if connections are recycled after delegate connection use, false otherwise
   */
  public boolean isRecycleAfterDelegateUse()
  {
    return recycleAfterDelegateUse;
  }

  /**
   * Specifies the minimum time interval between cleaning attempts of
   * the {@code Cleaner} thread.
   * @return The minimum time interval between cleaning attempts of
   * the {@code Cleaner} thread
   */
  @Override
  protected long getMinimumCleaningInterval()
  {
    return 1000L;
  }

  /**
   * Specifies the maximum time interval between cleaning attempts of
   * the {@code Cleaner} thread.
   * @return The maximum time interval between cleaning attempts of
   * the {@code Cleaner} thread
   */
  @Override
  protected long getMaximumCleaningInterval()
  {
    return 5000L;
  }

  //************************
  // Event-handling methods
  //************************

  /**
   * Adds a {@link ConnectionPoolListener} to the event notification list.
   * @param listener listener to add
   */
  public final void addConnectionPoolListener(ConnectionPoolListener listener)
  {
    listeners.add(listener);
  }

  /**
   * Removes a {@link ConnectionPoolListener} from the event notification list.
   * @param listener listener to remove
   */
  public final void removeConnectionPoolListener(ConnectionPoolListener listener)
  {
    listeners.remove(listener);
  }

  /**
   * Fires an ConnectionPoolEvent to all listeners.
   * 'type' should be one of ConnectionPoolEvent types.
   */
  private void firePoolEvent(int type)
  {
    if (listeners.isEmpty())
      return;
    ConnectionPoolEvent poolEvent = new ConnectionPoolEvent(this, type);
    // Setup event dispatch thread if necessary.
    if (eventDispatcher == null)
    {
      eventDispatcher = new EventDispatcher<>(listeners, new Notifier());
      eventDispatcher.start();
    }
    // Dispatch event.
    eventDispatcher.dispatchEvent(poolEvent);
  }

  /**
   * Fires a ConnectionPoolEvent.POOL_RELEASED event to all listeners.
   * This method performs the listener notification synchronously to ensure
   * all listeners receive the event before the event-dispatch thread is
   * shutdown.
   */
  private void firePoolReleasedEvent()
  {
    if (listeners.isEmpty())
      return;
    ConnectionPoolEvent poolEvent = new ConnectionPoolEvent(this, ConnectionPoolEvent.POOL_RELEASED);
    // No copy of listeners needs to be taken as the collection is thread-safe.
    for (ConnectionPoolListener listener : listeners)
    {
      try
      {
        listener.poolReleased(poolEvent);
      }
      catch (RuntimeException rx)
      {
        log_warn("Exception thrown by listener on pool release", rx);
      }
    }
  }


  /**
   * Default implementation of {@link ConnectionValidator}.
   * This class simply checks a Connection with the {@link java.sql.Connection#isClosed()} method.
   */
  private static final class DefaultValidator implements ConnectionValidator
  {
    /**
     * Validates a {@link Connection}.
     */
    @Override
    public boolean isValid(Connection con)
    {
      try
      {
        return (con instanceof CacheConnection) ? true : !con.isClosed();
      }
      catch (SQLException sqlx)
      {
        return false;
      }
    }
  }


  /**
   * Class to relay {@link ObjectPoolEvent} instances as
   * {@link ConnectionPoolEvent} instances.
   */
  private final class EventRelay<T extends CacheConnection> implements ObjectPoolListener<T>
  {
    @Override
    public void poolInitCompleted(ObjectPoolEvent<T> evt)
    {
      firePoolEvent(ConnectionPoolEvent.INIT_COMPLETED);
    }

    @Override
    public void poolCheckOut(ObjectPoolEvent<T> evt)
    {
      firePoolEvent(ConnectionPoolEvent.CHECKOUT);
    }

    @Override
    public void poolCheckIn(ObjectPoolEvent<T> evt)
    {
      firePoolEvent(ConnectionPoolEvent.CHECKIN);
    }

    @Override
    public void validationError(ObjectPoolEvent<T> evt)
    {
      firePoolEvent(ConnectionPoolEvent.VALIDATION_ERROR);
    }

    @Override
    public void maxPoolLimitReached(ObjectPoolEvent<T> evt)
    {
      firePoolEvent(ConnectionPoolEvent.MAX_POOL_LIMIT_REACHED);
    }

    @Override
    public void maxPoolLimitExceeded(ObjectPoolEvent<T> evt)
    {
      firePoolEvent(ConnectionPoolEvent.MAX_POOL_LIMIT_EXCEEDED);
    }

    @Override
    public void maxSizeLimitReached(ObjectPoolEvent<T> evt)
    {
      firePoolEvent(ConnectionPoolEvent.MAX_SIZE_LIMIT_REACHED);
    }

    @Override
    public void maxSizeLimitError(ObjectPoolEvent<T> evt)
    {
      firePoolEvent(ConnectionPoolEvent.MAX_SIZE_LIMIT_ERROR);
    }

    @Override
    public void poolParametersChanged(ObjectPoolEvent<T> evt)
    {
      firePoolEvent(ConnectionPoolEvent.PARAMETERS_CHANGED);
    }

    @Override
    public void poolFlushed(ObjectPoolEvent<T> evt)
    {
      firePoolEvent(ConnectionPoolEvent.POOL_FLUSHED);
    }

    @Override
    public void poolReleased(ObjectPoolEvent<T> evt)
    {
      firePoolReleasedEvent();
      listeners.clear();
    }
  }

  /**
   * {@link EventNotifier} implementation to notify event listeners of events.
   */
  private final class Notifier implements EventNotifier<ConnectionPoolListener, ConnectionPoolEvent>
  {
    @Override
    public void notifyListener(ConnectionPoolListener cpl, ConnectionPoolEvent evt)
    {
      try
      {
        switch (evt.getType())
        {
          case ConnectionPoolEvent.INIT_COMPLETED:
            cpl.poolInitCompleted(evt);
            break;
          case ConnectionPoolEvent.CHECKOUT:
            cpl.poolCheckOut(evt);
            break;
          case ConnectionPoolEvent.CHECKIN:
            cpl.poolCheckIn(evt);
            break;
          case ConnectionPoolEvent.VALIDATION_ERROR:
            cpl.validationError(evt);
            break;
          case ConnectionPoolEvent.MAX_POOL_LIMIT_REACHED:
            cpl.maxPoolLimitReached(evt);
            break;
          case ConnectionPoolEvent.MAX_POOL_LIMIT_EXCEEDED:
            cpl.maxPoolLimitExceeded(evt);
            break;
          case ConnectionPoolEvent.MAX_SIZE_LIMIT_REACHED:
            cpl.maxSizeLimitReached(evt);
            break;
          case ConnectionPoolEvent.MAX_SIZE_LIMIT_ERROR:
            cpl.maxSizeLimitError(evt);
            break;
          case ConnectionPoolEvent.PARAMETERS_CHANGED:
            cpl.poolParametersChanged(evt);
            break;
          case ConnectionPoolEvent.POOL_FLUSHED:
            cpl.poolFlushed(evt);
            break;
          case ConnectionPoolEvent.POOL_RELEASED:
            cpl.poolReleased(evt);
            break;
          default:
        }
      }
      catch (RuntimeException rx)
      {
        log_warn("Exception raised by pool listener", rx);
      }
    }
  }
}
