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

/**
 * Simple command-line application to output basic details of the host
 * Java Virtual Machine and its JDBC version compatibility.
 *
 * @author Giles Winstanley
 */
public class JDBCInfo
{
  /** Class name to test for JDBC4.0. */
  private static final String JDBC40_CLASSNAME = "java.sql.RowId";
  /** Class name to test for JDBC3.0. */
  private static final String JDBC30_CLASSNAME = "java.sql.Savepoint";
  /** Class name to test for JDBC2.1. */
  private static final String JDBC21_CLASSNAME = "java.sql.Struct";
  /** Class name to test for JDBC1.2. */
  private static final String JDBC12_CLASSNAME = "java.sql.Driver";
  /** JDBC major version number. */
  private static int verMajor = 0;
  /** JDBC minor version number. */
  private static int verMinor = 0;
  /** JDBC version string. */
  private static String verString = null;

  private JDBCInfo() {}

  /**
   * @return JDBC version string (e.g. &quot;4.0&quot;).
   */
  public static synchronized String getVersionString()
  {
    if (verString != null)
      return verString;
    else
    {
      findVersion();
      return verString;
    }
  }

  /**
   * @return JDBC major version number (e.g. &quot;2&quot; for JDBC 2.1).
   */
  public static synchronized int getMajorVersion()
  {
    if (verMajor > 0)
      return verMajor;
    else
    {
      findVersion();
      return verMajor;
    }
  }

  /**
   * @return JDBC minor version number (e.g. &quot;1&quot; for JDBC 2.1).
   */
  public static synchronized int getMinorVersion()
  {
    if (verMajor > 0)
      return verMinor;
    else
    {
      findVersion();
      return verMinor;
    }
  }

  /**
   * Method implementing logic to determine JDBC version.
   */
  private static synchronized void findVersion()
  {
    try
    {
      Class.forName(JDBC40_CLASSNAME);
      verMajor = 4;
    }
    catch (ClassNotFoundException cnfx) {}
    try
    {
      if (verMajor == 0)
      {
        Class.forName(JDBC30_CLASSNAME);
        verMajor = 3;
      }
    }
    catch (ClassNotFoundException cnfx) {}
    try
    {
      if (verMajor == 0)
      {
        Class.forName(JDBC21_CLASSNAME);
        verMajor = 2;
        verMinor = 1;
      }
    }
    catch (ClassNotFoundException cnfx) {}
    try
    {
      if (verMajor == 0)
      {
        Class.forName(JDBC12_CLASSNAME);
        verMajor = 1;
        verMinor = 2;
      }
    }
    catch (ClassNotFoundException cnfx) {}

    // Create version string from number.
    verString = verMajor + "." + verMinor;
  }

  public static void main(String[] args)
  {
    System.out.println();
    System.out.println("Operating System: " + System.getProperty("os.name"));
    System.out.println("OS Achitecture  : " + System.getProperty("os.arch"));
    System.out.println("OS Version      : " + System.getProperty("os.version"));
    System.out.println("Java Version    : " + System.getProperty("java.version"));
    System.out.println("Java Vendor     : " + System.getProperty("java.vendor"));
    System.out.println("Java VM Name    : " + System.getProperty("java.vm.name"));
    System.out.println();
    System.out.println("Your JVM supports JDBC " + getVersionString());
  }
}
