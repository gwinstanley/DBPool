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
package snaq.util.logging;

import java.text.DateFormat;

/**
 * Implementation of a simple single-line log
 * {@link java.util.logging.Formatter Formatter}
 * for use with the Java Logging API. It formats similarly to the logs of
 * pre-5.0 versions of DBPool, and is to help maintain some degree of
 * backwards-compatibility and ease of transition for upgraders who aren't
 * very good with change.
 * <p>In a Java Logging configuration properties file:
 * <pre>
 *     &lt;handler&gt;.formatter=snaq.util.logging.DBPoolLegacyFormatter
 * </pre>
 * <p>Note: this class will probably be removed in a future release,
 * and is here only to make the transition to the new logging framework
 * a bit smoother.</p>
 * 
 * @author Giles Winstanley
 */
public class DBPoolLegacyFormatter extends TerseFormatter
{
  public DBPoolLegacyFormatter()
  {
    super(false, false, false, false);
    setDateFormat(DateFormat.getDateTimeInstance(DateFormat.LONG, DateFormat.LONG));
  }
}
