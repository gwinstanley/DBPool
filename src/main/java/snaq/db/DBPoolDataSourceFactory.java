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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NamingException;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating instances of the {@link DBPoolDataSource} class.
 *
 * @author Giles Winstanley
 */
public class DBPoolDataSourceFactory implements ObjectFactory
{
  /** SLF4J shared instance for writing log entries. */
  protected static final Logger logger = LoggerFactory.getLogger(DBPoolDataSourceFactory.class);
  /** Counter for numbering unnamed DataSource instances. */
  private static final AtomicInteger counter = new AtomicInteger(0);

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
  @Override
  public Object getObjectInstance(final Object obj, final Name name, final Context nameCtx, final Hashtable<?,?> environment) throws Exception
  {
    if (logger.isDebugEnabled())
    {
      logger.debug("Object : " + obj);
      logger.debug("Name   : " + name + (name == null ? "" : (" (" + name.getClass().getName() + ")")));
      logger.debug("Context: " + nameCtx);
      if (environment != null)
      {
        List list = new ArrayList(environment.keySet());
        Collections.sort(list);
        for (Iterator it = list.iterator(); it.hasNext();)
        {
          Object o = it.next();
          logger.debug("Environment[" + o + "]: " + environment.get(o));
        }
      }
    }

    if (!(obj instanceof Reference))
      return null;

    // Create instance of DataSource.
    DBPoolDataSource ds = new DBPoolDataSource();
    if (name != null)
    {
      StringBuilder sb = new StringBuilder();
      for (Enumeration<String> e = name.getAll(); e.hasMoreElements(); )
        sb.append(e.nextElement());
      ds.setName(sb.toString());
    }
    else
    {
      ds.setName(Integer.toString(counter.getAndIncrement()));
    }

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
      {
        ds.setDriverClassName(refValue);
        logger.trace("Set DataSource description: " + refValue);
      }
      else if (refName.equalsIgnoreCase("user") || refName.equalsIgnoreCase("username"))
      {
        ds.setUser(refValue);
        logger.trace("Set DataSource username: " + refValue);
      }
      else if (refName.equalsIgnoreCase("password"))
      {
        ds.setPassword(refValue);
        logger.trace("Set DataSource password");
      }
      // ------------------------------------
      // DBPool custom DataSource properties.
      // ------------------------------------
      else if (refName.equalsIgnoreCase("driverClassName"))
      {
        ds.setDriverClassName(refValue);
        logger.trace("Set DataSource driver class name: " + refValue);
      }
      else if (refName.equalsIgnoreCase("url"))
      {
        ds.setUrl(refValue);
        logger.trace("Set DataSource URL: " + refValue);
      }
      else if (refName.equalsIgnoreCase("passwordDecoderClassName"))
      {
        ds.setPasswordDecoderClassName(refValue);
        logger.trace("Set DataSource PasswordDecoder class name: " + refValue);
      }
      else if (refName.equalsIgnoreCase("validatorClassName"))
      {
        ds.setValidatorClassName(refValue);
        logger.trace("Set DataSource ConnectionValidator class name: " + refValue);
      }
      else if (refName.equalsIgnoreCase("validationQuery"))
      {
        ds.setValidationQuery(refValue);
        logger.trace("Set DataSource validation query: " + refValue);
      }
      else if (refName.equalsIgnoreCase("minPool"))
      {
        try
        {
          ds.setMinPool(Integer.parseInt(refValue));
        }
        catch (NumberFormatException nfx)
        {
          throw new NamingException("Invalid '" + refName + "' value: " + refValue);
        }
        logger.trace("Set DataSource minPool: " + refValue);
      }
      else if (refName.equalsIgnoreCase("maxPool"))
      {
        try
        {
          ds.setMaxPool(Integer.parseInt(refValue));
        }
        catch (NumberFormatException nfx)
        {
          throw new NamingException("Invalid '" + refName + "' value: " + refValue);
        }
        logger.trace("Set DataSource maxPool: " + refValue);
      }
      else if (refName.equalsIgnoreCase("maxSize"))
      {
        try
        {
          ds.setMaxSize(Integer.parseInt(refValue));
        }
        catch (NumberFormatException nfx)
        {
          throw new NamingException("Invalid '" + refName + "' value: " + refValue);
        }
        logger.trace("Set DataSource maxSize: " + refValue);
      }
      else if (refName.equalsIgnoreCase("idleTimeout"))
      {
        try
        {
          ds.setIdleTimeout(Integer.parseInt(refValue));
        }
        catch (NumberFormatException nfx)
        {
          throw new NamingException("Invalid '" + refName + "' value: " + refValue);
        }
        logger.trace("Set DataSource idleTimeout: " + refValue);
      }
      else if (refName.equalsIgnoreCase("loginTimeout"))
      {
        try
        {
          ds.setLoginTimeout(Integer.parseInt(refValue));
        }
        catch (NumberFormatException nfx)
        {
          throw new NamingException("Invalid '" + refName + "' value: " + refValue);
        }
        logger.trace("Set DataSource loginTimeout: " + refValue);
      }
      else
      {
        ds.setConnectionProperty(refName, refValue);
        logger.trace(String.format("Set DataSource property: %s=%s", refName, refValue));
      }
    }

    return ds;
  }
}
