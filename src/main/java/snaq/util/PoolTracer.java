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
package snaq.util;

import snaq.util.logging.LogUtil;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Enumeration;
import java.util.Properties;

/**
 * Class to create a simple trace of pool usage statistics to a log file.
 * The default date format is local time in ISO 8601 compatible format.
 *
 * @author Giles Winstanley
 * @param <T> class type of pooled objects
 */
public class PoolTracer<T extends Reusable> implements ObjectPoolListener<T>
{
  /** Logging utility. */
  private final LogUtil logger = new LogUtil();
  /** Default DateFormat instance. */
  private static final DateFormat DEFAULT_DATEFORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
  /** Default format string for log messages. */
  private static final String DEFAULT_FORMAT_STRING =
          "name={0}, " +
          "minpool={1,number,#}, " +
          "maxpool={2,number,#}, " +
          "maxsize={3,number,#}, " +
          "idleTimeout={4,number,#}, " +
          "out={5,number,#}, " +
          "free={6,number,#}, " +
          "hitRate={8,number,0.0##%}, " +
          "event={9}";
  /** {@code MessageFormat} instance for preparing log messages. */
  private MessageFormat msgFormat;

  /**
   * Creates a new {@code PoolTracer} which logs to the specified
   * {@code PrintWriter}.
   * @param pool {@code ObjectPool} for which to trace activity
   * @param logWriter {@code PrintWriter} to use for writing trace activity
   * @param closeOnExit whether to close the {@code PrintWriter} on release
   */
  public PoolTracer(ObjectPool<T> pool, PrintWriter logWriter, boolean closeOnExit)
  {
    logger.setLog(logWriter, closeOnExit);
    logger.setDateFormat(DEFAULT_DATEFORMAT);
    setMessageFormat(null);
    pool.addObjectPoolListener(this);
  }

  /**
   * Creates a new {@code PoolTracer} which logs to the specified file.
   * Note: if the file already exists it is truncated to zero-length, then used
   * for writing the log.
   * @param pool {@code ObjectPool} for which to trace activity
   * @param file {@code File} to which to log trace activity
   * @throws FileNotFoundException if unable to find specified file
   */
  public PoolTracer(ObjectPool<T> pool, File file) throws FileNotFoundException
  {
    this(pool, new PrintWriter(file), true);
  }

  /**
   * Creates a new ObjectPoolAudit which logs to the specified File.
   * @param pool {@code ObjectPool} for which to trace activity
   * @param filename name of file to which to log trace activity
   * @throws FileNotFoundException if unable to find specified file
   */
  public PoolTracer(ObjectPool<T> pool, String filename) throws FileNotFoundException
  {
    this(pool, new PrintWriter(filename), true);
  }

  /**
   * Creates a new {@code PoolTracer} which logs to the specified file.
   * Note: if the file already exists it is truncated to zero-length, then used
   * for writing the log.
   * @param props {@code Properties} defining trace activity
   * @throws FileNotFoundException if unable to find file specified in properties
   */
  public PoolTracer(Properties props) throws FileNotFoundException
  {
    Properties p = convertToLC(props);
    // Find file property, and spew if not found.
    String filename = p.getProperty("file");
    if (filename == null || filename.equals(""))
      throw new IllegalArgumentException("Property not specified: file");
    logger.setLog(new PrintWriter(new File(filename)), true);

    String dateFormat = p.getProperty("dateformat");
    if (dateFormat != null && !dateFormat.trim().equals(""))
      logger.setDateFormat(new SimpleDateFormat(dateFormat));
    else
      logger.setDateFormat(DEFAULT_DATEFORMAT);

    String format = p.getProperty("format");
    if (format != null && !format.trim().equals(""))
      setMessageFormat(new MessageFormat(format));
    else
      setMessageFormat(null);
  }

  // Converts property keys to lower-case for easier redudant checking.
  private static Properties convertToLC(Properties p)
  {
    Properties props = new Properties();
    for (Enumeration<?> e = p.propertyNames(); e.hasMoreElements();)
    {
      String key = (String)e.nextElement();
      props.put(key.toLowerCase(), p.getProperty(key));
    }
    return props;
  }

  /**
   * Sets the date formatter for the logging.
   * The default is the same as used by the {@link LogUtil} class.
   * @param df {@code DateFormat} instance to use for formatting log messages
   */
  public synchronized void setDateFormat(DateFormat df)
  {
    logger.setDateFormat(df);
  }

  /**
   * Sets the {@code MessageFormat} instance used for formatting log messages.
   * To use the default instance, just call this method with a null argument.
   * <p>Format strings are specified using the following variables:</p>
   * <ul>
   * <li>{0} - pool name (string)</li>
   * <li>{1} - minpool (integer)</li>
   * <li>{2} - maxpool (integer)</li>
   * <li>{3} - maxsize (integer)</li>
   * <li>{4} - idle timeout (long)</li>
   * <li>{5} - items checked out (integer)</li>
   * <li>{6} - free item count (integer)</li>
   * <li>{7} - current pool size (i.e. {5}+{6}, integer)</li>
   * <li>{8} - hit rate (float)</li>
   * <li>{9} - event type (string)</li>
   * </ul>
   * <p>The default format is as follows:</p>
   * <pre style="font-size:80%;">
   * {0}: minpool={1,number,#}, maxpool={2,number,#}, maxsize={3,number,#}, idleTimeout={4,number,#}, out={5,number,#}, free={6,number,#}, hitRate={8,number,0.0##%}
   * </pre>
   * @param mf {@code MessageFormat} instance to use for formatting log messages
   */
  public final void setMessageFormat(MessageFormat mf)
  {
    msgFormat = (mf == null) ? new MessageFormat(DEFAULT_FORMAT_STRING) : mf;
  }

  /**
   * Returns the current {@code MessageFormat} instance used for formatting log messages.
   * @return The current {@code MessageFormat} instance used for formatting log messages
   */
  public MessageFormat getMessageFormatInstance()
  {
    return msgFormat;
  }

  /**
   * Writes an entry containing the pool statistics to the log file.
   * @param evt event instance to log
   */
  protected void logPoolStats(ObjectPoolEvent<T> evt)
  {
    Object[] o = new Object[10];
    o[0] = evt.getPool().getName();
    o[1] = evt.getMinPool();
    o[2] = evt.getMaxPool();
    o[3] = evt.getMaxSize();
    o[4] = evt.getIdleTimeout();
    o[5] = evt.getCheckedOut();
    o[6] = evt.getFreeCount();
    o[7] = evt.getSize();
    o[8] = evt.getPoolHitRate();
    o[9] = evt.getTypeString();
    String msg = msgFormat.format(o);
    logger.log(msg);
  }

  @Override
  public void poolInitCompleted(ObjectPoolEvent<T> evt)
  {
  }

  @Override
  public void validationError(ObjectPoolEvent<T> evt)
  {
  }

  @Override
  public void maxPoolLimitReached(ObjectPoolEvent<T> evt)
  {
  }

  @Override
  public void maxPoolLimitExceeded(ObjectPoolEvent<T> evt)
  {
  }

  @Override
  public void maxSizeLimitReached(ObjectPoolEvent<T> evt)
  {
  }

  @Override
  public void maxSizeLimitError(ObjectPoolEvent<T> evt)
  {
  }

  @Override
  public void poolCheckIn(ObjectPoolEvent<T> evt)
  {
    logPoolStats(evt);
  }

  @Override
  public void poolCheckOut(ObjectPoolEvent<T> evt)
  {
    logPoolStats(evt);
  }

  @Override
  public void poolParametersChanged(ObjectPoolEvent<T> evt)
  {
    logPoolStats(evt);
  }

  @Override
  public void poolFlushed(ObjectPoolEvent<T> evt)
  {
    logPoolStats(evt);
  }

  @Override
  public void poolReleased(ObjectPoolEvent<T> evt)
  {
    logPoolStats(evt);
    evt.getPool().removeObjectPoolListener(this);
    logger.close();
  }
}
