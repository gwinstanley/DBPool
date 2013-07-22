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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Command-line utility to send SQL commands to a database.
 * This class is useful for creating a large number of database tables
 * and/or records from a user-defined text file containing SQL commands.
 * It relies on the {@code ConnectionPoolManager} class to assist
 * with the creation of a connection to the database, which in turn requires
 * the appropriate {@code dbpool.properties} file in the classpath.
 * <pre>
 *     Usage: java snaq.db.SQLUpdate &lt;poolnames&gt; &lt;input file&gt; [&lt;separator&gt;]
 * </pre>
 * where {@code pool} is the name of the connection pool as defined in
 * the <em>dbpool.properties</em> file, {@code input file} is the name of the text
 * file containing the SQL statements to be issued to the defined database,
 * and {@code separator} is an optional parameter to specify a delimiter
 * for the SQL statements in the file. If the separator is not specified then
 * each line of the file is assumed to be a separate statement.
 * <p>
 * Note: comments are allowed in the input file by starting the line with
 * either &quot;#&quot; or &quot;--&quot;.
 *
 * @see snaq.db.ConnectionPoolManager
 * @author Giles Winstanley
 */
public class SQLUpdate
{
  /** Apache Commons Logging shared instance for writing log entries. */
  protected static final Log logger = LogFactory.getLog(SQLUpdate.class);
  /** Platform line-separator. */
  private final static String LSEP = System.getProperty("line.separator");
  /** Pool manager for defining database connections. */
  private ConnectionPoolManager cpm;
  /** Database connection for issuing SQL statements. */
  private Connection con;
  /** Statement to use for executing SQL. */
  private Statement statement;

  public SQLUpdate(String db) throws IOException
  {
    cpm = ConnectionPoolManager.getInstance();
  }

  /**
   * Opens a database connection using a specified connection pool.
   * @param poolname name of the connection pool from which to get a connection
   */
  private void openConnection(String poolname) throws SQLException
  {
    if (poolname == null || poolname.equals(""))
      throw new SQLException("Please specify the name of a defined connection pool");
    try
    {
      con = cpm.getConnection(poolname);
      statement = con.createStatement();
    }
    catch (SQLException sqlx)
    {
      try { statement.close(); }
      catch (SQLException sqlx2) {}
      try { con.close(); }
      catch (SQLException sqlx2) {}
    }
  }

  /**
   * Closes the current database connection.
   */
  private void closeConnection()
  {
    try { statement.close(); }
    catch (SQLException sqlx) { sqlx.printStackTrace(); }
    try { con.close(); }
    catch (SQLException sqlx) { sqlx.printStackTrace(); }
    cpm.release();
  }

  /**
   * Issues a statement to the database.
   */
  private void doStatement(String sql) throws SQLException
  {
    try
    {
      logger.debug("SQL: " + sql);
      statement.executeUpdate(sql);
    }
    catch (SQLException sqlx)
    {
      logger.info(sqlx.getMessage(), sqlx);
      throw sqlx;
    }
  }

  /**
   * Loads a text file into a string.
   * @param f {@link File} containing text to load
   * @return {@link String} containing the text loaded from the specified file
   */
  public static String loadTextFile(File f) throws IOException
  {
    FileInputStream fis = null;
    try
    {
      ByteArrayOutputStream bao = new ByteArrayOutputStream();
      fis = new FileInputStream(f);
      byte[] b = new byte[4096];
      int n;
      while ((n = fis.read(b)) != -1)
        bao.write(b, 0, n);
      return new String(bao.toByteArray());
    }
    finally
    {
      fis.close();
    }
  }

  /**
   * Splits the specified text input into separate SQL statements.
   * @param text string containing the text to be processed
   * @param separator string specifying the separator between SQL statements
   * @return {@link String} array containing the SQL statements parsed
   */
  public static String[] splitSQL(String text, String separator)
  {
    // Create list to hold SQL statements.
    List<String> list = new ArrayList<String>();
    if (separator == null)
    {
      StringTokenizer st = new StringTokenizer(text, "\n\r");
      while (st.hasMoreTokens())
      {
        String token = st.nextToken().trim();
        if (!token.startsWith("#") && !token.equals(""))
          list.add(token);
      }
    }
    else
    {
      StringBuffer sb = new StringBuffer();
      StringTokenizer st = new StringTokenizer(text, "\n\r");
      while (st.hasMoreTokens())
      {
        // Get next line.
        String line = st.nextToken();

        // If line is a comment...ignore it.
        if (line.startsWith("#") || line.startsWith("--"))
        {
          sb.setLength(0);
        }
        else
        {
          int pos = line.indexOf(separator);
          if (pos >= 0)
          {
            sb.append(line.substring(0, pos));
            list.add(sb.toString());
            sb.setLength(0);
          }
          else
            sb.append(line);
        }
      }
    }
    return list.toArray(new String[0]);
  }

  /**
   * Allows command-line issuing of SQL statements to a database defined
   * in the pool manager properties file.
   */
  public static void main(String args[])
  {
    String cn = SQLUpdate.class.getName();
    if (args == null || args.length < 2)
    {
      System.out.println("Usage: java " + cn + " <poolname> <text file> [<separator>]");
      System.exit(0);
    }

    String db = args[0];
    String file = args[1];
    String separator = args.length < 3 ? null : args[2];
    if (separator != null)
      System.out.println("Separator: " + separator);

    // Load file.
    String contents = null;
    try
    {
      contents = loadTextFile(new File(file));
    }
    catch (IOException iox)
    {
      System.out.println("I/O error with file " + file);
      iox.printStackTrace();
      System.exit(1);
    }

    // Split loaded text into SQL statements.
    String[] sql = splitSQL(contents, separator);

    // Open database connection, issue SQL, then close connection.
    SQLUpdate sqlUpdate = null;
    try
    {
      sqlUpdate = new SQLUpdate(db);
      sqlUpdate.openConnection(db);
      for (int i = 0; i < sql.length; i++)
        sqlUpdate.doStatement(sql[i]);
    }
    catch (IOException iox)
    {
      System.err.println("Unable to create instance of " + cn);
      iox.printStackTrace();
      System.exit(1);
    }
    catch (SQLException sqlx)
    {
      sqlx.printStackTrace();
    }
    finally
    {
      sqlUpdate.closeConnection();
    }
    System.out.println();
  }
}
