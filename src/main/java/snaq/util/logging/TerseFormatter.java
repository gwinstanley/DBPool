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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;

/**
 * Implementation of a simple single-line log
 * {@link java.util.logging.Formatter Formatter}
 * for use with the Java Logging API. This is useful for producing terse log
 * output, and can be used by setting the {@link java.util.logging.Formatter Formatter}
 * of a {@link java.util.logging.Handler Handler} to an instance of this class.
 * <p>In a Java Logging configuration properties file:
 * <pre>
 *     &lt;handler&gt;.formatter=snaq.util.logging.TerseFormatter
 * </pre>
<p>NOTE: this class is NOT thread-safe.
 *
 * @author Giles Winstanley
 * @deprecated This class may be removed in future, and users are encouraged to use SLF4J logging.
 */
@Deprecated
public class TerseFormatter extends SimpleFormatter
{
  /** Platform line-separator. */
  private final static String LSEP = System.getProperty("line.separator");
  /** Date instance for re-use in date/time formatting. */
  private final Date date = new Date();
  /** {@code DateFormat} instance for date/time formatting. */
  private DateFormat dateFormat;
  /** Separator string (between log-entry info and log message). */
  protected String separator;
  /** Log entry formatting string. */
  private String formatString;
  /** Argument array for log entry message formatting. */
  private final String args[] = new String[5];
  /** Determines whether to show class names in log entries. */
  private boolean showClass;
  /** Determines whether to show short class names in log entries. */
  private boolean showClassShort;
  /** Determines whether to show method names in log entries. */
  private boolean showMethod;
  /** Determines whether to show levels in log entries. */
  private boolean showLevel;

  /**
   * Creates a new TerseFormatter instance.
   * @param showLevel whether to display log levels
   * @param showClass whether to display class names
   * @param showClassShort whether to display short class names (i.e. no package details)
   * @param showMethod whether to display method names
   */
  public TerseFormatter(boolean showLevel, boolean showClass, boolean showClassShort, boolean showMethod)
  {
    this.showLevel = showLevel;
    this.showClass = showClass;
    this.showClassShort = showClassShort;
    this.showMethod = showMethod;
    createFormatString();
    // Default date format: ISO 8601
    dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
  }

  /**
   * Creates a new TerseFormatter instance.
   * The created instance displays log levels.
   * @param showClass whether to display class names
   * @param showClassShort whether to display short class names (i.e. no package details)
   * @param showMethod whether to display method names
   */
  public TerseFormatter(boolean showClass, boolean showClassShort, boolean showMethod)
  {
    this(true, showClass, showClassShort, showMethod);
  }

  /**
   * Creates a new TerseFormatter instance (all details displayed).
   */
  public TerseFormatter()
  {
    this(true, false, false, false);
  }

  /**
   * Sets the {@code DateFormat} instance to use for formatting log entries.
   * @param df {@code DateFormat} instance to use
   */
  public void setDateFormat(DateFormat df)
  {
    this.dateFormat = df;
  }

  /**
   * @return {@code DateFormat} instance used for formatting log entries.
   */
  public DateFormat getDateFormat()
  {
    return dateFormat;
  }

  /**
   * Sets the separator string between the date and log message (default &quot;: &quot;).
   * To set the default separator (&quot;: &quot;), call with a null argument.
   * @param sep string to use as separator
   */
  public synchronized void setSeparator(String sep)
  {
    separator = sep;
    createFormatString();
  }

  private void createFormatString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("{0}");
    if (showLevel)
      sb.append(" {1}");
    if (showClass)
    {
      sb.append(" ({2}");
      if (showMethod)
        sb.append("#{3}");
      sb.append(" )");
    }
    sb.append(separator == null ? ": " : separator);
    sb.append("{4}");
    sb.append(LSEP);
    formatString = sb.toString();
  }

  @Override
  public String format(LogRecord record)
  {
    // Format date/time.
    date.setTime(record.getMillis());
    if (dateFormat == null)
      dateFormat = DateFormat.getDateTimeInstance();

    // Set values of argument for log entry formatting.
    args[0] = dateFormat.format(date);
    args[1] = record.getLevel().getLocalizedName();
    if (showClass)
      args[2] = record.getSourceClassName();
    if (showMethod)
      args[3] = record.getSourceMethodName().trim();
    args[4] = formatMessage(record);

    // Check for short class name.
    if (showClassShort)
      args[2] = args[2].substring(args[2].lastIndexOf('.') + 1);

    // Create buffer to hold log entry message.
    StringBuilder sb = new StringBuilder();
    sb.append(MessageFormat.format(formatString, (Object[])args));

    // Check for Throwable attached to log record.
    if (record.getThrown() != null)
    {
      try
      {
        StringWriter sw = new StringWriter();
        try (PrintWriter pw = new PrintWriter(sw))
        {
          record.getThrown().printStackTrace(pw);
        }
        sb.append(sw.toString());
      }
      catch (Exception ex)
      {
        ex.printStackTrace();
      }
    }
    return sb.toString();
  }
}
