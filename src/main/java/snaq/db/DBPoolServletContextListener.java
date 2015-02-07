package snaq.db;

import javax.naming.InitialContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import snaq.db.DBPoolDataSource;

/**
 * ServletContextListener implementation to handle connection pool shutdown.
 * The JNDI name of the configured DBPoolDataSource should be assigned as a
 * context parameter for the web application.
 * For example, typically these lines might be added to {@code web.xml}:
 * <pre>
 *     &lt;listener&gt;
 *         &lt;listener-class&gt;snaq.db.DBPoolServletContextListener&lt;/listener-class&gt;
 *     &lt;/listener&gt;
 *     &lt;context-param&gt;
 *         &lt;param-name&gt;name&lt;/param-name&gt;
 *         &lt;param-value&gt;jdbc/pool-ds&lt;/param-value&gt;
 *     &lt;/context-param&gt;
 * </pre>
 * in the case that the DataSource has the JNDI name of <em>jdbc/pool-ds</em>.
 *
 * @author Giles Winstanley
 */
public class DBPoolServletContextListener implements ServletContextListener
{
  /** SLF4J logger instance for writing log entries. */
  private static final Logger log = LoggerFactory.getLogger(DBPoolServletContextListener.class);

  @Override
  public void contextInitialized(ServletContextEvent evt)
  {
  }

  @Override
  public void contextDestroyed(ServletContextEvent evt)
  {
    // Find configured parameter.
    String name = evt.getServletContext().getInitParameter("name");
    if (name == null || name.trim().isEmpty())
      log.warn("Found invalid 'name' parameter in ServletContext");
    try
    {
      // Find DataSource in JNDI context.
      InitialContext ctx = new InitialContext();
      Object o = ctx.lookup("java:comp/env/" + name);
      if (o == null || !(o instanceof DataSource))
      {
        log.warn("ServletContext 'name' parameter doesn't refer to a DataSource: " + o);
        return;
      }

      DBPoolDataSource ds = (DBPoolDataSource)o;
      log.trace(String.format("Found compatible DBPoolDataSource (%s): releasing", ds.getName()));
      ds.release();
    }
    catch (Throwable t)
    {
      log.warn(t.getMessage(), t);
    }
  }
}
