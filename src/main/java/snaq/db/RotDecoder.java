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
 * Decodes passwords using the simple
 * <a href="http://en.wikipedia.org/wiki/ROT13" target="wikiWin">ROT13</a> algorithm.
 * This algorithm is very insecure, and only included as an example.
 *
 * @author Giles Winstanley
 */
public class RotDecoder implements PasswordDecoder
{
  private static final int offset = 13;

  public char[] decode(String encoded)
  {
    StringBuffer sb = new StringBuffer(encoded);
    for (int a = 0; a < sb.length(); a++)
    {
      char c = sb.charAt(a);
      if (c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z')
      {
        char base = Character.isUpperCase(c) ? 'A' : 'a';
        int i = c - base;
        c = (char)(base + (i + offset) % 26);
        sb.setCharAt(a, c);
      }
    }
    char[] out = new char[sb.length()];
    sb.getChars(0, out.length, out, 0);
    return out;
  }

  /**
   * Method included as convenience for testing.
   */
  public static void main(String[] args) throws Exception
  {
    if (args.length < 1)
    {
      System.out.println("Usage: java RotDecoder <word> ...");
      System.exit(1);
    }
    RotDecoder x = new RotDecoder();
    for (int i = 0; i < args.length; i++)
      System.out.print(new String(x.decode(args[i])) + " ");
    System.out.println();
  }
}
