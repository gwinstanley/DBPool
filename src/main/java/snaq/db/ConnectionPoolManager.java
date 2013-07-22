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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import snaq.util.ObjectPoolListener;
import snaq.util.logging.LogUtil;

/**
 * <p>Class to provide access and management for multiple connection pools
 * defined in a properties file or object.
 * Clients get access to each defined instance through one of the
 * static {@code getInstance()} methods and can then check-out and check-in
 * database connections from the pools defined by that manager.
 * When the pool manager is no longer required the {@code release()} method
 * should be called to ensure all associated resources are released.
 * Once called, that pool manager instance can no longer be used.</p>
 *
 * <p>To facilitate easy release of any/all pool managers methods
 * {@link #registerShutdownHook} and {@link #registerGlobalShutdownHook} are
 * provided, which when called adds a hook to the Java Virtual Machine to
 * release one/all pool manager instance(s) within the context of the same
 * {@code ClassLoader}.</p>
 *
 * <p>Properties for a manager can be specified in three different ways.
 * <ol>
 * <li>Properties file located in CLASSPATH
 * <li>Properties file referenced explicitly (with a {@link File} object)
 * <li>{@link Properties} object
 * </ol>
 *
 * <ol>
 * <li>A CLASSPATH located properties file can simply be accessed using the
 * method {@code getInstance(name)} where <em>name</em> is the name of the
 * properties file specified as a string.
 * <li>To specify a properties file which is not in the CLASSPATH use the
 * method {@code getInstance(File)}. This same file handle must be used
 * each time you want to obtain the instance in this way.
 * <li>To specify the pools using a Properties object a call must be made to
 * the {@code createInstance(Properties)} method. This method creates the
 * ConnectionPoolManager instance and makes it available via the {@code getInstance()}
 * method.
 * </ol>
 * <p><b>Note:</b> The {@code getInstance()} method can return one of two
 * possible instances depending on the previous calls made to the pool manager.
 * If the {@code createInstance(Properties)} method has previously been
 * called successfully then it will return this manually created instance.
 * Otherwise it will attempt to return an instance relating to the default
 * properties file (dbpool.properties) within the CLASSPATH, if it exists.</p>
 *
 * <p>The properties given to the manager specify which JDBC drivers to use to
 * access the relevant databases, and also defines the characteristics of each
 * connection pool. The properties required/allowed are as follows
 * (those marked with * are mandatory):</p>
 * <pre>
 * name                            Name of this pool manager instance (for log identification)
 * drivers*                        Class names of required JDBC Drivers (comma/space delimited)
 * logfile                         Specifies a custom log file for this pool manager
 * dateformat                      {@link SimpleDateFormat} formatting string for custom log entries
 *
 * &lt;poolname&gt;.url*                 JDBC URL for the database
 * &lt;poolname&gt;.user                 Database username for login
 * &lt;poolname&gt;.password             Database password for login
 * &lt;poolname&gt;.minpool              Minimum number of pooled connections (0 if none)
 * &lt;poolname&gt;.maxpool              Maximum number of pooled connections (0 if none)
 * &lt;poolname&gt;.maxsize              Maximum number of possible connections (0 if no limit)
 * &lt;poolname&gt;.idleTimeout          Connection idle timeout time in seconds (0 if no timeout)
 * &lt;poolname&gt;.validator            Class name of {@link ConnectionValidator} to use
 * &lt;poolname&gt;.decoder              Class name of {@link PasswordDecoder} to use
 * &lt;poolname&gt;.prop.<em>XXX</em>             Passes property <em>XXX</em> and its value to the JDBC driver
 * &lt;poolname&gt;.logfile              Filename of logfile for this pool (optional)
 * &lt;poolname&gt;.dateformat           {@link SimpleDateFormat} formatting string for custom log entries
 * &lt;poolname&gt;.debug                Whether to log debug info (optional, default:false)
 * &lt;poolname&gt;.cache                Whether to cache Statements (default:true)
 * &lt;poolname&gt;.access               Pool access type ({LIFO, FIFO, RANDOM}, default:LIFO)
 * &lt;poolname&gt;.async                Whether to use asynchronous connection destruction (default:false)
 * &lt;poolname&gt;.recycleAfterRaw      Whether to turn on recycling of connections that have had delegate accessed (default:false)
 * &lt;poolname&gt;.listenerN            Class name of {@link ConnectionPoolListener} to create (N=0, 1, ...)
 * &lt;poolname&gt;.listenerN.XXX        Passes property XXX and its value to the numbered listener
 * &lt;poolname&gt;.mbean                Whether to register a JMX MBean for this pool (default:false)
 * </pre>
 *
 * <p>Multiple pools can be specified provided they each use a different pool name.
 * The {@code validator} property optionally specifies the name of a
 * class to be used for validating the database connections.</p>
 *
 * @see snaq.db.AutoCommitValidator
 * @see snaq.db.ConnectionValidator
 * @see <a href="http://en.wikipedia.org/wiki/LIFO_(computing)" target="wikiWin">LIFO</a>
 * @see <a href="http://en.wikipedia.org/wiki/FIFO_(computing)" target="wikiWin">FIFO</a>
 *
 * @author Giles Winstanley
 */
public final class ConnectionPoolManager implements Comparable<ConnectionPoolManager>
{
  /** Shared Apache Commons Logging instance for writing log entries (if logger name not specified). */
  private static Log loggerShared = LogFactory.getLog(ConnectionPoolManager.class);
  /** Apache Commons Logging instance for writing log entries. */
  private Log logger;
  /** Custom logging utility (backward-compatible). */
  private LogUtil logUtil;
  /** Map key for referring to class instance. */
  private static final String PROPERTIES_INSTANCE_KEY = "PROPERTIES_INSTANCE";
  /** Classpath reference to default properties file. */
  static final String DEFAULT_PROPERTIES_FILE = "/dbpool.properties";
  /** Default charset name for the properties file (same as platform default). */
  private static final String DEFAULT_CHARSET = Charset.defaultCharset().name();
  /** Hashtable to hold pool manager instances. */
  private static Hashtable<Object,ConnectionPoolManager> managers = new Hashtable<Object,ConnectionPoolManager>();
  /** List to hold references to JDBC drivers for this instance. */
  private Set<Driver> drivers = new HashSet<Driver>();
  /** Name of the pool manager. */
  private String name;
  /** Counter for naming unnamed pool managers. */
  private static int unnamedCount = 0;
  /** Thread to perform global shutdown of registered pool managers. */
  private static Thread shutdownHookGlobal = null;
  /** Thread to perform shutdown of this pool manager. */
  private Thread shutdownHook = null;
  /** Flag indicating whether this pool manager instance has been released. */
  private boolean released = false;
  /** Map of {@link ConnectionPool} instances being held. */
  private final Map<String,ConnectionPool> pools = new HashMap<String,ConnectionPool>();
  /** Holder for defining source of this pool manager instance. */
  private Object source;
  /** Key to refer to this pool manager instance within shared map. */
  private Object instanceKey;
  /** List to hold listeners for {@link ConnectionPoolManagerEvent} events. */
  private final List<ConnectionPoolManagerListener> listeners = new ArrayList<ConnectionPoolManagerListener>();

  private ConnectionPoolManager(Properties props, Object src)
  {
    super();
    this.source = src;
    init(props);
  }

  /**
   * Registers a shutdown hook for this ConnectionPoolManager instance
   * to ensure it is released if the JVM exits
   */
  public synchronized void registerShutdownHook()
  {
    if (shutdownHook != null)
      return;
    try
    {
      shutdownHook = new Releaser(this);
      Runtime.getRuntime().addShutdownHook(shutdownHook);
    }
    catch (Exception ex)
    {
      log_warn("Error registering shutdown-hook for " + this, ex);
    }
  }

  /**
   * Removes a registered shutdown hook for this ConnectionPoolManager instance.
   */
  public synchronized void removeShutdownHook()
  {
    try
    {
      if (shutdownHook != null)
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
      shutdownHook = null;
      log_info("Removed ConnectionPoolManager shutdown-hook");
    }
    catch (Exception ex)
    {
      log_warn("Error removing ConnectionPoolManager shutdown-hook", ex);
    }
  }

  /**
   * Registers a shutdown hook for all current and future ConnectionPoolManager
   * instances to ensure they are released if the JVM exits with any managers
   * having not been released.
   * <p><strong>Note:</strong> All individual shutdown hooks will be deregistered
   * and replaced with this single shutdown hook.
   */
  public static synchronized void registerGlobalShutdownHook()
  {
    if (shutdownHookGlobal != null)
      return;
    try
    {
      shutdownHookGlobal = new Releaser();
      Runtime.getRuntime().addShutdownHook(shutdownHookGlobal);
      loggerShared.info("Registered global ConnectionPoolManager shutdown-hook");
      // Remove instance hooks.
      for (ConnectionPoolManager cpm : getInstances())
        cpm.removeShutdownHook();
    }
    catch (Exception ex)
    {
      loggerShared.warn("Error registering global ConnectionPoolManager shutdown-hook", ex);
    }
  }

  /**
   * Removes a registered global shutdown hook for all current and future
   * ConnectionPoolManager instances.
   */
  public static void removeGlobalShutdownHook()
  {
    if (shutdownHookGlobal != null)
      Runtime.getRuntime().removeShutdownHook(shutdownHookGlobal);
    shutdownHookGlobal = null;
    loggerShared.info("Removed global ConnectionPoolManager shutdown-hook");
  }

  /** Returns a descriptive string for this instance. */
  @Override
  public String toString()
  {
    if (source instanceof String)
      return getClass().getName() + "[CLASSPATH resource:" + source + "]";
    else if (source instanceof File)
      return getClass().getName() + "[File:" + ((File)source).getAbsolutePath() + "]";
    else if (source instanceof Properties)
      return getClass().getName() + "[Properties]";
    else
      return getClass().getName() + "[Unknown]";
  }

  /**
   * Indicates whether some other object is &quot;equal to&quot; this one.
   * This implementation performs checks on the name and the
   * &quot;source&quot;, which represents how the pool manager was created.
   */
  @Override
  public boolean equals(Object obj)
  {
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    final ConnectionPoolManager other = (ConnectionPoolManager)obj;
    if ((this.name == null) ? (other.name != null) : !this.name.equals(other.name))
      return false;
//    if (this.toString() != other.toString() && (this.toString() == null || !this.toString().equals(other.toString())))
//      return false;
    if (this.source != other.source && (this.source == null || !this.source.equals(other.source)))
      return false;
    return true;
  }

  /**
   * Returns a hash code value for the object.
   * This implementation hashes on the name and {@link #toString()} value.
   */
  @Override
  public int hashCode()
  {
    int hash = 3;
    hash = 71 * hash + (this.name != null ? this.name.hashCode() : 0);
    hash = 71 * hash + (this.toString() != null ? this.toString().hashCode() : 0);
    return hash;
  }

  /**
   * Compares this object with the specified object for order.
   * This implementation is generally consistent with the implementation of
   * {@link #equals(Object)}, but consistency is not complete due to
   * awkwardness of comparison of {@code Properties} instances.
   * (check source for implementation details).
   */
  public int compareTo(ConnectionPoolManager cpm)
  {
    if (cpm == null)
      throw new NullPointerException();
    int i = this.name.compareTo(cpm.getName());
    if (i != 0)
      return i;

    if (source instanceof String)
      i = ((String)source).compareTo(((String)cpm.source));
    else if (source instanceof File)
      i = ((File)source).compareTo(((File)cpm.source));
    else if (source instanceof Properties)
    {
      Properties p1 = (Properties)source;
      Properties p2 = (Properties)cpm.source;
      i = -Integer.valueOf(p1.size()).compareTo(Integer.valueOf(p2.size()));
    }
    return i;
  }

  /**
   * Returns a {@link Set} containing all the current {@code ConnectionPoolManager} instances.
   * This method is included for convenience for external monitoring.
   * Clients wanting to obtain an instance for using connections should NOT use this method.
   * @return all current instances of {@code ConnectionPoolManager}.
   */
  public static Set<ConnectionPoolManager> getInstances()
  {
    return new HashSet<ConnectionPoolManager>(managers.values());
  }

  /**
   * Returns the singleton instance of the {@code ConnectionPoolManager} for the specified properties file.
   * @param propsFile filename of the properties file to use (path info should not be specified; available CLASSPATH will be searched for the properties file)
   * @param enc character-encoding to use for reading properties file
   * @return instance of {@code ConnectionPoolManager} relating to the specified properties file
   * @throws IOException if there was an problem loading the properties
   */
  public static synchronized ConnectionPoolManager getInstance(String propsFile, String enc) throws IOException
  {
    if (propsFile == null)
      throw new IllegalArgumentException();
    String s = propsFile.startsWith("/") ? propsFile : ("/" + propsFile);
    Object o = managers.get(s);
    ConnectionPoolManager cpm = (o != null) ? (ConnectionPoolManager)o : null;
    if (cpm == null || cpm.isReleased())
    {
      Properties props = loadProperties(s, enc);
      if (props == null)
        throw new FileNotFoundException("Unable to find properties file: " + propsFile);
      cpm = new ConnectionPoolManager(props, propsFile);
      cpm.instanceKey = s;
      managers.put(cpm.instanceKey, cpm);
      cpm.fireInstancesChangedEvent();
    }
    return cpm;
  }

  /**
   * Returns the singleton instance of the {@code ConnectionPoolManager} for the specified properties file.
   * @param propsFile filename of the properties file to use (path info should not be specified; available CLASSPATH will be searched for the properties file)
   * @return instance of {@code ConnectionPoolManager} relating to the specified properties file
   * @throws IOException if there was an problem loading the properties
   */
  public static synchronized ConnectionPoolManager getInstance(String propsFile) throws IOException
  {
    return getInstance(propsFile, DEFAULT_CHARSET);
  }

  /**
   * Returns the singleton instance of the ConnectionPoolManager for the specified properties file.
   * @param propsFile filename of the properties file to use (path info should not be specified; available CLASSPATH will be searched for the properties file)
   * @param enc character-encoding to use for reading properties file
   * @return instance of ConnectionPoolManager relating to the specified properties file
   * @throws IOException if there was an problem loading the properties
   */
  public static synchronized ConnectionPoolManager getInstance(File propsFile, String enc) throws IOException
  {
    Object o = managers.get(propsFile);
    ConnectionPoolManager cpm = (o != null) ? (ConnectionPoolManager)o : null;
    if (cpm == null || cpm.isReleased())
    {
      try
      {
        cpm = new ConnectionPoolManager(loadProperties(propsFile, enc), propsFile);
        cpm.instanceKey = propsFile;
        managers.put(cpm.instanceKey, cpm);
        cpm.fireInstancesChangedEvent();
      }
      catch (IOException iox)
      {
        if (iox instanceof FileNotFoundException)
          loggerShared.warn("Unable to find the properties file " + propsFile.getAbsolutePath(), iox);
        else
          loggerShared.warn("Error loading the properties file " + propsFile.getAbsolutePath(), iox);
        return null;
      }
    }
    return cpm;
  }

  /**
   * Returns the singleton instance of the ConnectionPoolManager for the specified properties file.
   * @param propsFile filename of the properties file to use (path info should not be specified; available CLASSPATH will be searched for the properties file)
   * @return instance of ConnectionPoolManager relating to the specified properties file
   * @throws IOException if there was an problem loading the properties
   */
  public static synchronized ConnectionPoolManager getInstance(File propsFile) throws IOException
  {
    return getInstance(propsFile, DEFAULT_CHARSET);
  }

  /**
   * Returns the standard singleton instance of the {@code ConnectionPoolManager}.
   * If an instance has been obtained with a user-specified {@link Properties} object
   * then this instance is returned, otherwise an attempt is made to return an
   * instance using the default properties file ({@code dbpool.properties}).
   * @throws IOException if there was an problem loading the properties
   */
  public static synchronized ConnectionPoolManager getInstance() throws IOException
  {
    Object o = managers.get(PROPERTIES_INSTANCE_KEY);
    ConnectionPoolManager cpm = (o != null) ? (ConnectionPoolManager)o : null;
    if (cpm == null || cpm.isReleased())
      cpm = getInstance(DEFAULT_PROPERTIES_FILE);
    return cpm;
  }

  /**
   * Creates a singleton instance of the {@code ConnectionPoolManager} for the specified
   * {@link Properties} object. To subsequently use this instance user's should call the
   * {@link #getInstance()} method. This mechanism is used to provide the maximum
   * separation between creation and use of this instance to avoid haphazard
   * changes to any referenced {@code Properties} object that may occur between calls.
   * (This method can only be used successfully if no default properties
   * instance exists and is in use at the time of calling.)
   * @param props {@code Properties} object to use
   * @throws RuntimeException if default properties instance already exists and is in use
   */
  public static synchronized void createInstance(Properties props)
  {
    // Check for presence of default properties file instance.
    Object o = managers.get(DEFAULT_PROPERTIES_FILE);
    ConnectionPoolManager cpm = (o != null) ? (ConnectionPoolManager)o : null;
    if (cpm != null && !cpm.isReleased())
      throw new RuntimeException("Default properties file instance already exists");

    // Create new instance and store reference.
    cpm = new ConnectionPoolManager(props, props);
    cpm.instanceKey = PROPERTIES_INSTANCE_KEY;
    managers.put(cpm.instanceKey, cpm);
    cpm.fireInstancesChangedEvent();
  }

  /**
   * Loads and returns a {@code Properties} object from file.
   * @param propsFile properties file
   * @param enc character encoding to use for properties
   */
  private static Properties loadProperties(File propsFile, String enc) throws IOException
  {
    if (!propsFile.exists())
      throw new FileNotFoundException(propsFile.getAbsolutePath() + " does not exist");
    if (propsFile.isDirectory())
      throw new IOException("Error accessing properties file - " + propsFile.getAbsolutePath() + " is a directory");

    InputStream is = new FileInputStream(propsFile);
    Properties props = null;
    try
    {
      props = loadProperties(is, enc);
    }
    catch (IOException iox)
    {
      loggerShared.warn("Unable to find the properties file " + propsFile.getAbsolutePath(), iox);
      throw iox;
    }
    return props;
  }

  /**
   * Loads and returns a {@code Properties} object from the resource specified.
   * The resource should be located in the current CLASSPATH to be found.
   * @param propsResource resource string to use to find properties file
   * @throws IOException if there was an problem loading the properties
   */
  static Properties loadProperties(String propsResource) throws IOException
  {
    return loadProperties(propsResource, DEFAULT_CHARSET);
  }

  /**
   * Loads and returns a {@code Properties} object from the resource specified.
   * The resource should be located in the current CLASSPATH to be found.
   * @param propsResource resource string to use to find properties file
   * @param enc character encoding to use for properties
   * @throws IOException if there was an problem loading the properties
   */
  static Properties loadProperties(String propsResource, String enc) throws IOException
  {
    InputStream is = ConnectionPoolManager.class.getResourceAsStream(propsResource);
    if (is == null)
      throw new FileNotFoundException("Unable to find properties file: " + propsResource);
    Properties props = null;
    try
    {
      props = loadProperties(is, enc);
    }
    catch (IOException iox)
    {
      loggerShared.warn("Unable to load the properties file. Make sure " + propsResource + " is in the CLASSPATH.", iox);
      throw iox;
    }
    return props;
  }

  /**
   * Loads and returns a {@code Properties} object from an {@code InputStream}.
   * This method also aims to improve properties file resilience by loading
   * property names/values in the UTF-8 character encoding.
   * @param is InputStream from which load Properties
   * @param enc character encoding to use for properties
   */
  private static Properties loadProperties(InputStream is, String enc) throws IOException
  {
    Properties props = new Properties();
    BufferedReader br = null;
    try
    {
      // Loads properties from file.
      br = new BufferedReader(new InputStreamReader(is, enc));
      String line = null;
      while ((line = br.readLine()) != null)
      {
        if (line.trim().startsWith("#"))
          continue;
        int epos = line.indexOf('=');
        if (epos > 0)
        {
          String key = line.substring(0, epos).trim();
          String val = line.substring(epos + 1).trim();
          props.setProperty(key, val);
        }
      }
    }
    finally
    {
      if (br != null)
        br.close();
    }
    loggerShared.info("Loaded ConnectionPoolManager properties with encoding " + enc);
    if (loggerShared.isTraceEnabled())
    {
      final String LSEP = System.getProperty("line.separator");
      SortedSet<Object> keys = new TreeSet<Object>(props.keySet());
      StringBuilder sb = new StringBuilder();
      sb.append("Properties read:");
      sb.append(LSEP);
      for (Object o : keys)
      {
        String key = (String)o;
        sb.append('\t');
        sb.append(key);
        sb.append('=');
        sb.append(props.getProperty(key));
        sb.append(LSEP);
      }
      loggerShared.trace(sb.toString());
    }
    return props;
  }

  /**
   * Processes the specified {@code Properties} instance to facilitate
   * parsing of {@code ConnectionPool} definitions.
   * It simply removes case-sensitivity from all property keys
   * (It does not change the case of any additional properties specified for
   * the JDBC Driver, or the names of pools.)
   * @param props Properties instance to be processed
   */
  private static Properties processProperties(Properties props)
  {
    Properties propsP = new Properties();
    // Process non-driver properties to be case-insensitive.
    final Pattern dp = Pattern.compile("^([^.]+)(\\.prop\\.)(.+)$", Pattern.CASE_INSENSITIVE);
    final Pattern pp = Pattern.compile("^([^.]+)((?:\\.[^.]+)+)$");
    for (Enumeration e = props.propertyNames(); e.hasMoreElements();)
    {
      String key = (String)e.nextElement();
      String newkey = null;
      Matcher mdp = dp.matcher(key);
      Matcher mpp = pp.matcher(key);
      if (mdp.matches())
        newkey = mdp.group(1) + mdp.group(2).toLowerCase() + mdp.group(3);
      else if (mpp.matches())
        newkey = mpp.group(1).toLowerCase() + mpp.group(2).toLowerCase();

      if (newkey != null && !key.equals(newkey))
        propsP.put(newkey, props.getProperty(key));
      else
        propsP.put(key, props.getProperty(key));
    }
    if (loggerShared.isTraceEnabled())
    {
      final String LSEP = System.getProperty("line.separator");
      SortedSet<Object> keys = new TreeSet<Object>(propsP.keySet());
      StringBuilder sb = new StringBuilder();
      sb.append("Properties processed:");
      sb.append(LSEP);
      for (Object o : keys)
      {
        String key = (String)o;
        sb.append('\t');
        sb.append(key);
        sb.append('=');
        sb.append(propsP.getProperty(key));
        sb.append(LSEP);
      }
      loggerShared.trace(sb.toString());
    }
    return propsP;
  }

  /**
   * Initializes this instance with values from the given {@code Properties} object.
   */
  private void init(Properties p)
  {
    Properties props = processProperties(p);
    // Create a unique logger if a name is specified, otherwise use shared one.
    name = props.getProperty("name");
    if (name == null || name.equals(""))
      name = "unknown" + unnamedCount++;
    logger = LogFactory.getLog(getClass().getName() + "." + name);

    // Create a custom logger if requested.
    String logFile = props.getProperty("logfile");
    String df = props.getProperty("dateformat");
    if (logFile != null)
    {
      try
      {
        logUtil = new LogUtil(new File(logFile));
        if (df != null)
          logUtil.setDateFormat(new SimpleDateFormat(df));
        else
          logUtil.setDateFormat(DateFormat.getDateTimeInstance(DateFormat.LONG, DateFormat.LONG));
      }
      catch (IOException e)
      {
        System.err.println("Can't open the log file: " + logFile);
      }
    }

    loadDrivers(props);
    createPools(props);
  }

  /**
   * Loads and registers all JDBC drivers. This is done by the
   * {@code ConnectionPoolManager} as opposed to the {@link ConnectionPool},
   * since many pools may share the same driver.
   * @param props the connection pool properties
   */
  private void loadDrivers(Properties props)
  {
    String driverClasses = props.getProperty("drivers");
    StringTokenizer st = new StringTokenizer(driverClasses, ",: \t\n\r\f");
    Enumeration<Driver> current = DriverManager.getDrivers();
    while (st.hasMoreElements())
    {
      String driverClassName = st.nextToken().trim();
      try
      {
        // Check if driver already registered.
        boolean using = false;
        while (current.hasMoreElements())
        {
          String cName = current.nextElement().getClass().getName();
          if (cName.equals(driverClassName))
            using = true;
        }
        if (!using)
        {
          Driver driver = (Driver)Class.forName(driverClassName).newInstance();
          DriverManager.registerDriver(driver);
          drivers.add(driver);
          log_info("Registered JDBC driver " + driverClassName);
        }
      }
      catch (Exception ex)
      {
        log_warn("Unable to register JDBC driver: " + driverClassName, ex);
      }
    }
  }

  /**
   * Creates instances of {@link ConnectionPool} based on the {@link Properties}
   * object. The supplied properties have been pre-processed by the
   * {@link #loadProperties(InputStream, String)} method to ensure the
   * property keys are case-insensitive, where applicable.
   * @param props the connection pool properties
   */
  private void createPools(Properties props)
  {
    for (Iterator iter = props.keySet().iterator(); iter.hasNext();)
    {
      String propKey = (String)iter.next();
      if (propKey.endsWith(".url"))
      {
        String poolName = propKey.substring(0, propKey.lastIndexOf("."));
        String url = props.getProperty(propKey);
        if (url == null || "".equals(url.trim()))
        {
          log_warn("No URL specified for " + poolName);
          continue;
        }

        // "Standard" properties.
        String user = props.getProperty(poolName + ".user");
        user = (user != null) ? user.trim() : user;
        String pass = props.getProperty(poolName + ".password");
        pass = (pass != null) ? pass.trim() : pass;
        String pMinPool = props.getProperty(poolName + ".minpool", "0").trim();
        String pMaxPool = props.getProperty(poolName + ".maxpool", "0").trim();
        String pMaxConn = props.getProperty(poolName + ".maxconn", "0").trim();  // Deprecated, but checked for backwards-compatibility.
        String pMaxSize = props.getProperty(poolName + ".maxsize");
        if (pMaxSize != null) pMaxSize = pMaxSize.trim();
        String pExpiry = props.getProperty(poolName + ".expiry", "0").trim();  // Deprecated, but checked for backwards-compatibility.
        String pIdleTimeout = props.getProperty(poolName + ".idletimeout");
        if (pIdleTimeout != null) pIdleTimeout = pIdleTimeout.trim();
        String validator = props.getProperty(poolName + ".validator");
        validator = (validator != null) ? validator.trim() : validator;
        String decoder = props.getProperty(poolName + ".decoder");
        String pInit = props.getProperty(poolName + ".init", "0").trim();
        // "Advanced" properties.
        boolean noCache = props.getProperty(poolName + ".cache", "true").trim().equalsIgnoreCase("false");
        String access = props.getProperty(poolName + ".access");
        boolean async = props.getProperty(poolName + ".async", "false").trim().equalsIgnoreCase("true");
        boolean recycleAfterDelegateUse = props.getProperty(poolName + ".recycleafterdelegateuse", "false").trim().equalsIgnoreCase("true");
        boolean mbean = props.getProperty(poolName + ".mbean", "false").trim().equalsIgnoreCase("true");
        // Custom logging properties.
        String logFile = props.getProperty(poolName + ".logfile");
        String dateformat = props.getProperty(poolName + ".dateformat");
        boolean poolDebug = props.getProperty(poolName + ".debug", "false").trim().equalsIgnoreCase("true");

        // Properties to be passed to JDBC Driver.
        Properties poolProps = new Properties();
        String prefix = poolName + ".prop.";
        Iterator it = props.keySet().iterator();
        while (it.hasNext())
        {
          String s = (String)it.next();
          if (s.startsWith(prefix))
            poolProps.setProperty(s.substring(prefix.length()), props.getProperty(s));
        }
        if (!poolProps.isEmpty() && user != null && !user.equals(""))
        {
          poolProps.setProperty("user", user);
          poolProps.setProperty("password", pass);
        }
        else
          poolProps = null;

        int minPool, maxPool, maxSize, idleTimeout;
        // Validate minpool.
        try
        {
          minPool = Integer.parseInt(pMinPool);
        }
        catch (NumberFormatException nfx)
        {
          log_warn("Invalid minpool value " + pMinPool + " for " + poolName);
          minPool = 0;
        }
        // Validate maxpool.
        try
        {
          maxPool = Integer.parseInt(pMaxPool);
        }
        catch (NumberFormatException nfx)
        {
          log_warn("Invalid maxpool value " + pMaxPool + " for " + poolName);
          maxPool = 0;
        }
        // Validate maxsize.
        try
        {
          maxSize = Integer.parseInt(pMaxSize);
        }
        catch (NumberFormatException nfx)
        {
          try
          {
            maxSize = Integer.parseInt(pMaxConn);
            log_warn("maxconn property has been deprecated; use maxsize instead");
          }
          catch (NumberFormatException nfx2)
          {
            if (pMaxSize != null)
              log_warn("Invalid maxsize value " + pMaxSize + " for " + poolName);
            else if (pMaxConn != null)
              log_warn("Invalid maxconn value " + pMaxConn + " for " + poolName);
            maxSize = 0;
          }
        }
        // Validate init (NOTE: 'init' property is deprecated).
        int initSize = 0;
        try
        {
          initSize = Integer.parseInt(pInit);
        }
        catch (NumberFormatException nfx)
        {
          log_warn("Invalid init value " + pInit + " for " + poolName);
          initSize = 0;
        }
        // Validate idle timeout.
        try
        {
          idleTimeout = Integer.parseInt(pIdleTimeout);
        }
        catch (NumberFormatException nfx)
        {
          try
          {
            idleTimeout = Integer.parseInt(pExpiry);
            log_warn("expiry property has been deprecated; use idleTimeout instead");
          }
          catch (NumberFormatException nfx2)
          {
            if (pIdleTimeout != null)
              log_warn("Invalid idleTimeout value " + pIdleTimeout + " for " + poolName);
            else if (pExpiry != null)
              log_warn("Invalid expiry value " + pExpiry + " for " + poolName);
            idleTimeout = 0;
          }
        }

        // Validate pool size logic.
        minPool = Math.max(minPool, 0);  // (ensure pMin >= 0).
        maxPool = Math.max(maxPool, 0);  // (ensure pMax >= 0).
        maxSize = Math.max(maxSize, 0);  // (ensure mSize >= 0).
        if (maxSize > 0)  // (if mSize > 0, ensure mSize >= pMax).
          maxSize = Math.max(maxSize, maxPool);
        idleTimeout = Math.max(idleTimeout, 0);  // (ensure timeout >= 0).

        // Create connection pool.
        ConnectionPool pool = null;
        if (poolProps != null)
          pool = new ConnectionPool(poolName, minPool, maxPool, maxSize, idleTimeout, url, poolProps);
        else
          pool = new ConnectionPool(poolName, minPool, maxPool, maxSize, idleTimeout, url, user, pass);

        // Setup JMX MBean access to pool (if requested).
        if (mbean)
          pool.registerMBean();

        //--- Custom logging configuration ---
        // Setup pool logging (pool-specific if specified, otherwise generic logfile).
        if (logFile != null && !logFile.equals(""))
        {
          try
          {
            File f = new File(logFile);
            if (f.exists() && f.isDirectory())
              log_warn("Invalid logfile specified for pool " + poolName + " - specified file is a directory");
            else if (!f.exists() && !f.createNewFile())
              log_warn("Invalid logfile specified for pool " + poolName + " - cannot create file " + f.getAbsolutePath());
            pool.setLog(new PrintWriter(new FileOutputStream(f, true), true));
          }
          catch (IOException iox)
          {
            log_warn("Invalid logfile specified for pool " + poolName, iox);
            if (logUtil != null)
              pool.setLog(logUtil.getLogWriter());
          }
        }
        else if (logUtil != null)
          pool.setLog(logUtil.getLogWriter());
        if (poolDebug)
          log_info("Enabling debug info on pool " + poolName);
        if (logUtil != null)
          logUtil.setDebug(poolDebug);
        // Set custom logging date format, if applicable.
        if (dateformat != null && !dateformat.equals(""))
        {
          try
          {
            DateFormat df = new SimpleDateFormat(dateformat);
            pool.getCustomLogger().setDateFormat(df);
          }
          catch (Exception ex)
          {
            log_warn("Invalid dateformat string specified: " + dateformat);
          }
        }
        //--- End Custom logging configuration ---

        if (noCache)
          log_info("Disabling caching on pool " + poolName);
        pool.setCaching(!noCache);
        if (async)
          log_info("Enabling asynchronous destruction on pool " + poolName);
        pool.setAsyncDestroy(async);
        if (recycleAfterDelegateUse)
          log_info("Enabling recycling after raw connection use on pool " + poolName);
        pool.setRecycleAfterDelegateUse(recycleAfterDelegateUse);

        // Set pool object selection method, if applicable.
        if (access != null && !access.equals(""))
        {
          try
          {
            if (access.equalsIgnoreCase("random"))
              pool.setPoolAccessRandom();
            else if (access.equalsIgnoreCase("fifo"))
              pool.setPoolAccessFIFO();
            else
              pool.setPoolAccessLIFO();
          }
          catch (Exception ex)
          {
            log_warn("Invalid access string specified: " + access);
          }
        }

        // Setup connection validator for pool.
        if (validator != null && !validator.equals(""))
        {
          try
          {
            Object o = Class.forName(validator).newInstance();
            if (o instanceof ConnectionValidator)
              pool.setValidator((ConnectionValidator)o);
          }
          catch (Exception ex)
          {
            log_warn("Unable to instantiate validator class for pool " + poolName + ": " + validator, ex);
          }
        }

        // Setup password decoder for pool.
        if (decoder != null && !decoder.equals(""))
        {
          try
          {
            Object o = Class.forName(decoder).newInstance();
            if (o instanceof PasswordDecoder)
              pool.setPasswordDecoder((PasswordDecoder)o);
          }
          catch (Exception ex)
          {
            log_warn("Unable to instantiate password decoder class for pool " + poolName + ": " + decoder, ex);
          }
        }

        // Add new pool to collection, and show summary info.
        synchronized(pools) { pools.put(poolName, pool); }
        String info = null;
        synchronized(pool)
        {
          info = "minpool=" + pool.getMinPool() + ",maxpool=" + pool.getMaxPool() + ",maxsize=" + pool.getMaxSize() + ",idleTimeout=";
          info += pool.getIdleTimeout() == 0 ? "none" : pool.getIdleTimeout();
        }
        log_info("Created pool " + poolName + " (" + info + ")");

        // Parse pool listeners from properties.
        Collection<ObjectPoolListener> poolListeners = parseListeners(props, poolName);
        for (ObjectPoolListener x : poolListeners)
        {
          if (x instanceof ConnectionPoolListener)
            pool.addConnectionPoolListener((ConnectionPoolListener)x);
          else
            pool.addObjectPoolListener(x);
        }

        // Setup initial connections in pool.
        // NOTE: This property has been retained for backwards-compatibility,
        // but is generally not needed. If 'minpool' is specified, the pool
        // will be auto-initialized to this level, so using the 'init'
        // property is only useful for initializing connections when 'minpool'
        // is zero.
        if (initSize > 0 && props.getProperty(poolName + ".minpool") == null)
          pool.init(initSize);
        else
          pool.init();
      }
    }
  }

  /**
   * Parses the specified Properties object for listeners.
   * Listeners are specified by:
   *   poolname.listenerN=class
   *   poolname.listenerN.prop.foo=bar
   * with N starting at 0 and working upwards.
   * @param props Properties instance with configuration info
   */
  @SuppressWarnings("unchecked")
  private Collection<ObjectPoolListener> parseListeners(Properties props, String poolName)
  {
    List<ObjectPoolListener> poolListeners = new ArrayList<ObjectPoolListener>();
    
    // Find listeners with no custom properties.
    String noPropListeners = props.getProperty(poolName + ".listeners");
    if (noPropListeners != null && !noPropListeners.equals(""))
    {
      StringTokenizer st = new StringTokenizer(noPropListeners, ",: \t\n\r\f");
      while (st.hasMoreTokens())
      {
        String x = st.nextToken();
        try
        {
          Object o = Class.forName(x).newInstance();
          if (o instanceof ObjectPoolListener)
          {
            poolListeners.add((ObjectPoolListener)o);
            log_trace("Added no-property PoolListener: " + x);
          }
        }
        catch (Exception ex)
        {
          log_warn("Unable to instantiate listener class for pool " + poolName + ": " + x, ex);
        }
      }
    }
    // Find listeners with potential custom properties.
    int count = 0;
    String propListener = props.getProperty(poolName + ".listener" + count);
    while (propListener != null && !propListener.trim().equals(""))
    {
      try
      {
        // Find class and check if it's a valid listener class.
        Class c = Class.forName(propListener);
        if (ObjectPoolListener.class.isAssignableFrom(c))
        {
          Constructor<? extends ObjectPoolListener> conP = null;
          Constructor<? extends ObjectPoolListener> conB = null;
          // Check for constructor that takes Properties instance.
          try { conP = c.getConstructor(new Class[] { Properties.class }); }
          catch (NoSuchMethodException nsmx) {}
          // Check for no-arg constructor.
          try { conB = c.getConstructor(new Class[] {}); }
          catch (NoSuchMethodException nsmx) {}
          // Check for custom properties.
          Properties lp = new Properties();
          if (conP != null)
          {
            String prefix = poolName + ".listener" + count + ".";
            for (Enumeration e = props.propertyNames(); e.hasMoreElements();)
            {
              String propKey = (String)e.nextElement();
              if (propKey.startsWith(prefix))
                lp.setProperty(propKey.substring(prefix.length()), props.getProperty(propKey));
            }
          }
          if (conP != null && conB == null || conP != null && conB != null && !lp.isEmpty())
          {
            poolListeners.add(conP.newInstance(new Object[] { lp }));
          }
          else if (conB != null)
          {
            poolListeners.add(conB.newInstance());
          }
          else
            log_warn("Unable to instantiate listener class for pool " + poolName + ": " + propListener);
        }
        else
        {
          log_warn("Listener class specified is not a valid listener: " + propListener);
        }
      }
      catch (Exception ex)
      {
        log_warn("Unable to instantiate listener class for pool " + poolName + ": " + propListener, ex.getCause());
      }
      // Increase counter.
      count++;
      propListener = props.getProperty(poolName + ".listener" + count);
    }
    // Return compiled set of listeners.
    return poolListeners;
  }

  /**
   * Compares two Properties instances for similarity, and returns a new
   * instance only containing those that differ in value.
   * Only property keys present in both p1 and p2 or just p2 are included.
   * @param p1 Base Properties instance to compare against
   * @param p2 Updated Properties instance to test
   */
  private final static Properties compareProperties(Properties p1, Properties p2)
  {
    Properties dp = new Properties();
    for (Enumeration e = p2.propertyNames(); e.hasMoreElements();)
    {
      String key = (String)e.nextElement();
      String p2v = p2.getProperty(key);
      if (p1.containsKey(key))
      {
        if (!p2v.equals(p1.getProperty(key)))
          dp.setProperty(key, p2v);
      }
      else
        dp.setProperty(key, p2v);
    }
    return dp;
  }

  /**
   * Returns the pool manager's name.
   */
  public final String getName() { return this.name; }

  /**
   * Returns one of the {@link ConnectionPool} instances by name.
   * (This is only provided as a convenience method to allow fine-tuning in
   * exceptional circumstances.)
   * @param name pool name as defined in the properties file
   * @return the pool, or {@code null} if the named pool could not be found
   * @throws IllegalArgumentException if the specified name is not a valid pool name (i.e. null)
   */
  public ConnectionPool getPool(String name)
  {
    if (released)
      throw new RuntimeException("Pool manager no longer valid for use");
    if (name == null || "".equals(name))
      throw new IllegalArgumentException("Invalid pool name specified: " + name);
    return pools.get(name);
  }

  /**
   * Returns all the current {@link ConnectionPool} instances maintained by this manager.
   * (This is only provided as a convenience method.)
   * @return array of {@code ConnectionPool} instances
   */
  public Collection<ConnectionPool> getPools()
  {
    synchronized(pools)
    {
      return Collections.unmodifiableCollection(pools.values());
    }
  }

  /**
   * Returns an open {@link Connection} from the specified {@link ConnectionPool}.
   * If one is not available, and the max number of connections has not been
   * reached, a new connection is created.
   * @param name {@code ConnectionPool} name as defined in the properties file
   * @return a {@code Connection}, or {@code null} if unable to obtain one
   * @throws IllegalArgumentException if the specified name is not a valid pool name
   * @throws SQLException if such an exception is raised by {@link ConnectionPool#getConnection()}
   */
  public Connection getConnection(String name) throws SQLException
  {
    if (released)
      throw new RuntimeException("Pool manager no longer valid for use");
    if (name == null || "".equals(name))
      throw new IllegalArgumentException("Invalid pool name specified: " + name);
    ConnectionPool pool = pools.get(name);
    if (pool == null)
      throw new IllegalArgumentException("Pool " + name + " not found");
    return pool.getConnection();
  }

  /**
   * Returns an open {@link Connection} from the specified pool.
   * If one is not available, and the max number of connections has not been
   * reached, a new connection is created. If the max number has been
   * reached, waits until one is available or the specified time has elapsed.
   * @param name pool name as defined in the properties file
   * @param timeout number of milliseconds to wait
   * @return the {@code Connection} or {@code null}
   * @throws IllegalArgumentException if the specified name is not a valid pool name
   * @throws SQLException if such an exception is raised by {@link ConnectionPool#getConnection(long)}
   */
  public Connection getConnection(String name, long timeout) throws SQLException
  {
    if (released)
      throw new RuntimeException("Pool manager no longer valid for use");
    if (name == null || "".equals(name))
      throw new IllegalArgumentException("Invalid pool name specified: " + name);
    if (timeout < 0)
      throw new IllegalArgumentException("Invalid timeout value specified: " + timeout);
    ConnectionPool pool = pools.get(name);
    if (pool == null)
      throw new IllegalArgumentException("Pool " + name + " not found");
    return pool.getConnection(timeout);
  }

  /**
   * Releases all resources for this {@code ConnectionPoolManager}, and deregisters
   * JDBC drivers if necessary. Any connections still in use are forcibly closed.
   */
  public synchronized void release()
  {
    if (isReleased())
      return;
    // Set released flag to prevent check-out of new items.
    released = true;

    synchronized(pools)
    {
      for (ConnectionPool pool : pools.values())
      {
        pool.releaseForcibly();
      }
    }

    // Check which drivers can be safely deregistered.
    for (ConnectionPoolManager cpm : managers.values())
    {
      if (!cpm.equals(this))
        drivers.removeAll(cpm.drivers);
    }
    for (Driver driver : drivers)
    {
      try
      {
        DriverManager.deregisterDriver(driver);
        log_info("Deregistered JDBC driver " + driver.getClass().getName());
      }
      catch (SQLException sqlx)
      {
        log_warn("Unable to deregister JDBC driver: " + driver.getClass().getName(), sqlx);
      }
    }
    // Remove this manager from those referenced.
    managers.remove(this.instanceKey);
    // Notify listeners.
    fireReleasedEvent();
    fireInstancesChangedEvent();
  }

  /**
   * Returns whether this instance has been released (and therefore is unusable).
   */
  public synchronized boolean isReleased() { return this.released; }

  /** Logging relay method (to prefix pool name). */
  protected void log_error(String s)
  {
    String msg = name + ": " + s;
    logger.error(msg);
    if (logUtil != null)
      logUtil.log(msg);
  }

  /** Logging relay method (to prefix pool name). */
  protected void log_error(String s, Throwable throwable)
  {
    String msg = name + ": " + s;
    logger.error(msg, throwable);
    if (logUtil != null)
      logUtil.log(msg, throwable);
  }

  /** Logging relay method (to prefix pool name). */
  protected void log_warn(String s)
  {
    String msg = name + ": " + s;
    logger.warn(msg);
    if (logUtil != null)
      logUtil.log(msg);
  }

  /** Logging relay method (to prefix pool name). */
  protected void log_warn(String s, Throwable throwable)
  {
    String msg = name + ": " + s;
    logger.warn(msg, throwable);
    if (logUtil != null)
      logUtil.log(msg, throwable);
  }

  /** Logging relay method (to prefix pool name). */
  protected void log_info(String s)
  {
    String msg = name + ": " + s;
    logger.info(msg);
    if (logUtil != null)
      logUtil.log(msg);
  }

  /** Logging relay method (to prefix pool name). */
  protected void log_info(String s, Throwable throwable)
  {
    String msg = name + ": " + s;
    logger.info(msg, throwable);
    if (logUtil != null)
      logUtil.log(msg, throwable);
  }

  /** Logging relay method (to prefix pool name). */
  protected void log_debug(String s)
  {
    String msg = name + ": " + s;
    logger.debug(msg);
    if (logUtil != null)
      logUtil.debug(msg);
  }

  /** Logging relay method (to prefix pool name). */
  protected void log_debug(String s, Throwable throwable)
  {
    String msg = name + ": " + s;
    logger.debug(msg, throwable);
    if (logUtil != null)
      logUtil.debug(msg, throwable);
  }

  /** Logging relay method (to prefix pool name). */
  protected void log_trace(String s)
  {
    logger.trace(name + ": " + s);
  }

  /** Logging relay method (to prefix pool name). */
  protected void log_trace(String s, Throwable throwable)
  {
    logger.trace(name + ": " + s, throwable);
  }

  //************************
  // Event-handling methods
  //************************
  /**
   * Adds a {@link ConnectionPoolManagerListener} to the event notification list.
   */
  public final void addConnectionPoolManagerListener(ConnectionPoolManagerListener x)
  {
    listeners.add(x);
  }

  /**
   * Removes a {@link ConnectionPoolManagerListener} from the event notification list.
   */
  public final void removeConnectionPoolManagerListener(ConnectionPoolManagerListener x)
  {
    listeners.remove(x);
  }

  private final void fireInstancesChangedEvent()
  {
    if (listeners.isEmpty())
      return;
    ConnectionPoolManagerEvent event = new ConnectionPoolManagerEvent(this);
    List<ConnectionPoolManagerListener> x = null;
    synchronized(listeners) { x = new ArrayList<ConnectionPoolManagerListener>(listeners); }
    for (ConnectionPoolManagerListener cpml : x)
      cpml.poolManagerInstancesChanged(event);
  }

  private final void fireReleasedEvent()
  {
    if (listeners.isEmpty())
      return;
    ConnectionPoolManagerEvent event = new ConnectionPoolManagerEvent(this);
    List<ConnectionPoolManagerListener> x = null;
    synchronized(listeners) { x = new ArrayList<ConnectionPoolManagerListener>(listeners); }
    for (ConnectionPoolManagerListener cpml : x)
      cpml.poolManagerReleased(event);
    listeners.clear();
  }

  //***************
  // Inner classes
  //***************
  /**
   * Utility class to release pool manager instances (used by shutdown-hook).
   */
  private static final class Releaser extends Thread
  {
    private ConnectionPoolManager instance;

    private Releaser()
    {
      setDaemon(true);
    }

    private Releaser(ConnectionPoolManager cpm)
    {
      instance = cpm;
    }

    @Override
    public void run()
    {
      if (instance == null)
      {
        for (ConnectionPoolManager cpm : getInstances())
        {
          if (!cpm.isReleased())
            cpm.release();
        }
      }
      else
      {
        if (!instance.isReleased())
          instance.release();
      }
    }
  }
}
