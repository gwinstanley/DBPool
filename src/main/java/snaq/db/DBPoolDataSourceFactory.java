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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NamingException;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Factory for creating instances of the {@link DBPoolDataSource} class.
 *
 * @author Giles Winstanley
 */
public class DBPoolDataSourceFactory implements ObjectFactory
{
  /** Apache Commons Logging shared instance for writing log entries. */
  protected static final Log logger = LogFactory.getLog(DBPoolDataSourceFactory.class);

  /**
   * Creates a {@link DBPoolDataSource} instance using the location or reference information specified.
   * The {@link ConnectionPool} parameters are extracted from the {@code obj}
   * reference object.
   * @param obj {@link Reference} object containing information for creating the {@code ConnectionPool}
   * @param name name of this object relative to {@code nameCtx}, or null if no name is specified
   * @param nameCtx context relative to which the {@code name} parameter is specified, or null if {@code name} is relative to the default initial context
   * @param environment possibly null environment that is used in creating the object
   * @return The {@code DBPoolDataSource} instance as specified, or null if it cannot be found/created.
   * @throws Exception if this object factory encountered an exception while attempting to create the {@code DBPoolDataSource}, and no other object factories are to be tried.
   */
  @SuppressWarnings("unchecked")
  public Object getObjectInstance(final Object obj, final Name name, final Context nameCtx, final Hashtable<?,?> environment) throws Exception
  {
    if (logger.isDebugEnabled())
    {
      logger.debug("Object : " + obj);
      logger.debug("Name   : " + name + " (" + name.getClass().getName() + ")");
      logger.debug("Context: " + nameCtx);
      List list = new ArrayList(environment.keySet());
      Collections.sort(list);
      for (Iterator it = list.iterator(); it.hasNext();)
      {
        Object o = it.next();
        logger.debug("Environment[" + o + "]: " + environment.get(o));
      }
    }

    if (!(obj instanceof Reference))
      return null;

    // Create instance of DataSource.
    DBPoolDataSource ds = new DBPoolDataSource();
    ds.registerShutdownHook();

    // Extract DataSource parameters from environment.
    Reference ref = (Reference)obj;

    for (Enumeration<RefAddr> e = ref.getAll(); e.hasMoreElements();)
    {
      RefAddr addr = e.nextElement();
      String refName = addr.getType();
      String refValue = (String)addr.getContent();

      // -------------------------------
      // Standard DataSource properties.
      // -------------------------------
      if (refName.equalsIgnoreCase("description"))
        ds.setDriverClassName(refValue);
      else if (refName.equalsIgnoreCase("user") || refName.equalsIgnoreCase("username"))
        ds.setUser(refValue);
      else if (refName.equalsIgnoreCase("password"))
        ds.setPassword(refValue);
      // ------------------------------------
      // DBPool custom DataSource properties.
      // ------------------------------------
      else if (refName.equalsIgnoreCase("driverClassName"))
        ds.setDriverClassName(refValue);
      else if (refName.equalsIgnoreCase("url"))  // If specified, overrides: networkProtocol/serverName/portNumber/databaseName
        ds.setUrl(refValue);
      else if (refName.equalsIgnoreCase("passwordDecoderClassName"))
        ds.setPasswordDecoderClassName(refValue);
      else if (refName.equalsIgnoreCase("validatorClassName"))
        ds.setValidatorClassName(refValue);
      else if (refName.equalsIgnoreCase("validationQuery"))
        ds.setValidationQuery(refValue);
      else if (refName.equalsIgnoreCase("minPool"))
      {
        try { ds.setMinPool(Integer.parseInt(refValue)); }
        catch (NumberFormatException nfx) { throw new NamingException("Invalid '" + refName + "' value: " + refValue); }
      }
      else if (refName.equalsIgnoreCase("maxPool") || refName.equalsIgnoreCase("poolSize"))
      {
        if (refName.equalsIgnoreCase("poolSize"))
          logger.warn("Attribute 'poolSize' is deprecated; use 'maxPool' instead");
        try { ds.setMaxPool(Integer.parseInt(refValue)); }
        catch (NumberFormatException nfx) { throw new NamingException("Invalid '" + refName + "' value: " + refValue); }
      }
      else if (refName.equalsIgnoreCase("maxSize") || refName.equalsIgnoreCase("maxConn"))
      {
        if (refName.equalsIgnoreCase("maxConn"))
          logger.warn("Attribute 'maxConn' is deprecated; use 'maxSize' instead");
        try { ds.setMaxSize(Integer.parseInt(refValue)); }
        catch (NumberFormatException nfx) { throw new NamingException("Invalid '" + refName + "' value: " + refValue); }
      }
      else if (refName.equalsIgnoreCase("idleTimeout") || refName.equalsIgnoreCase("expiryTime"))
      {
        if (refName.equalsIgnoreCase("expiryTime"))
          logger.warn("Attribute 'expiryTime' is deprecated; use 'idleTimeout' instead");
        try { ds.setIdleTimeout(Integer.parseInt(refValue)); }
        catch (NumberFormatException nfx) { throw new NamingException("Invalid '" + refName + "' value: " + refValue); }
      }
      else if (refName.equalsIgnoreCase("loginTimeout"))
      {
        try { ds.setLoginTimeout(Integer.parseInt(refValue)); }
        catch (NumberFormatException nfx) { throw new NamingException("Invalid '" + refName + "' value: " + refValue); }
      }
      else
      {
        logger.info("Unknown reference '" + refName + "' with value: " + refValue);
      }
    }

    return ds;
  }
}
