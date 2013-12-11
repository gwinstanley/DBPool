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

/**
 * Time-tracking wrapper class for an object.
 * 
 * @param <E> class type of object to wrap
 * @see snaq.util.ObjectPool
 * @author Giles Winstanley
 */
public class TimeWrapper<E>
{
  /** Object to be held in this wrapper instance. */
  private final E obj;
  /** Time at which this object expires. */
  private long expiryTime = 0L;
  /** Last access time (updated by method call). */
  private long accessed;

  /**
   * Creates a new wrapped object.
   * @param obj object to be referenced
   * @param expiry object's idle time before death in milliseconds (0 - eternal)
   */
  public TimeWrapper(E obj, long expiry)
  {
    this.obj = obj;
    this.accessed = System.currentTimeMillis();
    if (expiry > 0)
      this.expiryTime = this.accessed + expiry;
  }

  /**
   * Returns the object referenced by this wrapper.
   * NOTE: this does not update the last access time, which must be done
   * explicitly with the {@link #updateAccessed()} method.
   * @return The object referenced by this wrapper
   */
  public E getObject()
  {
    return obj;
  }

  /**
   * Whether this item has expired.
   * @return true if item has expired, false otherwise
   */
  public synchronized boolean isExpired()
  {
    return expiryTime > 0 && System.currentTimeMillis() > expiryTime;
  }

  /**
   * Sets idle time allowed before this item expires.
   * @param expiryTime idle time before expiry (0 = eternal)
   */
  synchronized void setLiveTime(long expiry)
  {
    if (expiry < 0)
      throw new IllegalArgumentException("Invalid expiry time");
    else if (expiry > 0)
      this.expiryTime = System.currentTimeMillis() + expiry;
    else
      this.expiryTime = 0;
  }

  /**
   * Updates the time this object was last accessed.
   */
  synchronized void updateAccessed()
  {
    accessed = System.currentTimeMillis();
  }

  /**
   * Returns the time this object was last accessed.
   * NOTE: this does not update the last access time, which must be done
   * explicitly with the {@link #updateAccessed()} method.
   */
  synchronized long getAccessed()
  {
    return accessed;
  }
}
