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
package snaq.util.logging;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.util.Date;

/**
 * Class providing simple logging and debug functionality,
 * which can be easily instantiated and used as a logging object.
 * This class is not related to other external logging libraries, and is used
 * to provide a standalone solution for writing basic log files.
 *
 * @author Giles Winstanley
 */
public class LogUtil
{
  /** {@code DateFormat} instance for formatting log entries. */
  private DateFormat dateFormat;
  /** Default {@code DateFormat} instance, used when custom one not set. */
  private DateFormat ddf;
  /** {@code PrintWriter} instance used for logging. */
  private PrintWriter logWriter;
  /** Flag determining whether log entries are written to the log stream. */
  private boolean logging = false;
  /** Flag determining whether the {@code LogWriter} is closed when {@link #close()} method is called. */
  private boolean closeWriterOnExit = true;
  /** Separator string (between date and log message). */
  private String separator = ": ";
  /** Flag to provide basic support for debug information (not used within class). */
  private boolean debug = false;

  /**
   * Creates a new Logger with logging disabled.
   */
  public LogUtil()
  {
  }

  /**
   * Creates a new {@code Logger} which writes to the specified file.
   * @param file file to which to write log entries
   * @throws FileNotFoundException if specified file is a directory, or cannot
   * be opened for some reason.
   */
  public LogUtil(File file) throws IOException
  {
    try
    {
      setLog(new PrintWriter(new FileOutputStream(file, true), true));
    }
    catch (FileNotFoundException fnfx)
    {
      throw (IOException)fnfx;
    }
  }

  /**
   * Sets the date formatter for the logging.
   * The default format is obtained using the method call:
   * {@code DateFormat.getDateTimeInstance(DateFormat.LONG, DateFormat.LONG)}
   * @param df {@code DateFormat} instance to use for formatting log messages
   * @see java.text.DateFormat
   */
  public synchronized void setDateFormat(DateFormat df)
  {
    dateFormat = df;
  }

  /**
   * Sets the separator string between the date and log message (default &quot;: &quot;).
   * To set the default separator, call with a null argument.
   * @param sep string to use as separator
   */
  public synchronized void setSeparator(String sep)
  {
    separator = (sep == null) ? ": " : sep;
  }

  /**
   * Sets the log stream and enables logging.
   * By default the {@code PrintWriter} is closed when the {@link #close()}
   * method is called.
   * @param writer {@code PrintWriter} to which to write log entries
   */
  public final synchronized void setLog(PrintWriter writer)
  {
    setLog(writer, true);
  }

  /**
   * Sets the log stream and enables logging.
   * @param writer {@code PrintWriter} to which to write log entries
   * @param closeOnExit whether to close the {@code PrintWriter} when {@link #close()} is called
   */
  public synchronized void setLog(PrintWriter writer, boolean closeOnExit)
  {
    if (logWriter != null)
    {
      logWriter.flush();
      if (closeWriterOnExit)
        close();
    }
    if (logging = (writer != null))
      logWriter = writer;
    else
      logWriter = null;
    this.closeWriterOnExit = (logWriter != null) ? closeOnExit : false;
  }

  /**
   * Returns the current {@code PrintWriter} used to write to the log.
   * @return The current {@code PrintWriter} used to write to the log
   */
  public synchronized PrintWriter getLogWriter()
  {
    return logWriter;
  }

  /**
   * Writes a message to the log.
   * @param logEntry message to write as a log entry
   */
  protected synchronized void writeLogEntry(String logEntry)
  {
    if (!logging)
      return;
    StringBuilder sb = new StringBuilder();
    Date date = new Date();
    if (dateFormat != null)
      sb.append(dateFormat.format(date));
    else
    {
      if (ddf == null)
        ddf = DateFormat.getDateTimeInstance(DateFormat.LONG, DateFormat.LONG);
      sb.append(ddf.format(date));
    }
    if (separator != null)
      sb.append(separator);
    sb.append(logEntry);
    logWriter.println(sb.toString());
  }

  /**
   * If debug is enabled, writes a message to the log.
   * @param logEntry message to write as a log entry
   */
  public synchronized void debug(String logEntry)
  {
    if (isDebug())
      writeLogEntry(logEntry);
  }

  /**
   * If debug is enabled, writes a message to the log with an optional prefix.
   * @param prefix prefix string for the log entry
   * @param logEntry message to write as a log entry
   */
  public synchronized void debug(String prefix, String logEntry)
  {
    if (isDebug())
      log(prefix, logEntry);
  }

  /**
   * If debug is enabled, writes a message with a {@code Throwable} to the log file.
   * @param logEntry message to write as a log entry
   * @param throwable {@code Throwable} instance to log with this entry
   */
  public void debug(String logEntry, Throwable throwable)
  {
    if (isDebug())
      log(logEntry, throwable);
  }

  /**
   * Writes a message to the log.
   * @param logEntry message to write as a log entry
   */
  public synchronized void log(String logEntry)
  {
    writeLogEntry(logEntry);
  }

  /**
   * Writes a message to the log with an optional prefix.
   * @param prefix prefix string for the log entry
   * @param logEntry message to write as a log entry
   */
  public synchronized void log(String prefix, String logEntry)
  {
    if (prefix == null || prefix.equals(""))
      writeLogEntry(logEntry);
    else
    {
      StringBuilder sb = new StringBuilder();
      sb.append(prefix);
      sb.append(logEntry);
      writeLogEntry(sb.toString());
    }
  }

  /**
   * Writes a message with a {@code Throwable} to the log file.
   * @param prefix prefix string for the log entry
   * @param logEntry message to write as a log entry
   * @param throwable {@code Throwable} instance to log with this entry
   */
  public synchronized void log(String prefix, String logEntry, Throwable throwable)
  {
    if (!logging)
      return;
    if (prefix == null || prefix.equals(""))
      log(logEntry, throwable);
    else
    {
      StringBuilder sb = new StringBuilder();
      sb.append(prefix);
      sb.append(logEntry);
      log(sb.toString(), throwable);
    }
  }

  /**
   * Writes a message with a {@code Throwable} to the log file.
   * @param throwable {@code Throwable} instance to log with this entry
   * @param logEntry message to write as a log entry
   */
  public synchronized void log(String logEntry, Throwable throwable)
  {
    if (!logging)
      return;
    writeLogEntry(logEntry);
    if (throwable != null)
    {
      throwable.printStackTrace(logWriter);
      logWriter.flush();
    }
  }

  /**
   * Writes a {@code Throwable} to the log file.
   * @param throwable {@code Throwable} instance to log with this entry
   */
  public void log(Throwable throwable)
  {
    log(throwable.getMessage(), throwable);
  }

  /**
   * Closes the log.
   */
  public synchronized void close()
  {
    logging = false;
    if (logWriter != null)
    {
      logWriter.flush();
      if (closeWriterOnExit)
        logWriter.close();
    }
    logWriter = null;
  }

  /**
   * Determines whether calls to the logging methods actually write to the log.
   * @param b flag indicating whether to write to the log
   */
  public synchronized void setLogging(boolean b)
  {
    logging = b;
  }

  /**
   * Returns whether calls to the logging methods actually write to the log.
   * @return true if logging is enabled, false otherwise.
   */
  public synchronized boolean isLogging()
  {
    return logging;
  }

  /**
   * Determines whether to perform debug logging.
   * @param b flag indicating whether to perform debug logging
   */
  public synchronized void setDebug(boolean b)
  {
    debug = b;
  }

  /**
   * Returns whether debug logging is enabled.
   * @return true if debug logging is enabled, false otherwise.
   */
  public synchronized boolean isDebug()
  {
    return debug;
  }
}
