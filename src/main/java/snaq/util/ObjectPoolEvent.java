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
package snaq.util;

import java.util.EventObject;

/**
 * Event for {@link ObjectPool} instances.
 * Convenience methods are available for determining the type fo the event,
 * and the event is capable of keeping shadow copies of the pool's vital
 * parameters at the time the event was issued, for reference to listeners.
 * 
 * @author Giles Winstanley
 */
public class ObjectPoolEvent extends EventObject
{
  public static final int INIT_COMPLETED = 1;
  public static final int CHECKOUT = 2;
  public static final int CHECKIN = 3;
  public static final int VALIDATION_ERROR = 4;
  public static final int MAX_POOL_LIMIT_REACHED = 5;
  public static final int MAX_POOL_LIMIT_EXCEEDED = 6;
  public static final int MAX_SIZE_LIMIT_REACHED = 7;
  public static final int MAX_SIZE_LIMIT_ERROR = 8;
  public static final int PARAMETERS_CHANGED = 9;
  public static final int POOL_FLUSHED = 10;
  public static final int POOL_RELEASED = 11;

  /** Event type of this instance. */
  private int type;

  // Shadow variables to hold copies at time of event dispatch.
  private int minPool;
  private int maxPool;
  private int maxSize;
  private long idleTimeout;
  private int checkedOut;
  private int freeCount;
  private int size;
  private float hitRate;

  /**
   * Creates a new {@code PoolEvent}.
   */
  public ObjectPoolEvent(ObjectPool pool, int type)
  {
    super(pool);
    this.type = type;
  }

  /**
   * Returns the pool for which this event was created.
   */
  public ObjectPool getPool() { return (ObjectPool)getSource(); }

  /**
   * Returns the type of event this object represents.
   */
  public int getType() { return type; }

  /**
   * Returns the type of event this object represents as a string.
   */
  public String getTypeString()
  {
    switch(type)
    {
      case INIT_COMPLETED: return "INIT_COMPLETED";
      case CHECKOUT: return "CHECKOUT";
      case CHECKIN: return "CHECKIN";
      case VALIDATION_ERROR: return "VALIDATION_ERROR";
      case MAX_POOL_LIMIT_REACHED: return "MAX_POOL_LIMIT_REACHED";
      case MAX_POOL_LIMIT_EXCEEDED: return "MAX_POOL_LIMIT_EXCEEDED";
      case MAX_SIZE_LIMIT_REACHED: return "MAX_SIZE_LIMIT_REACHED";
      case MAX_SIZE_LIMIT_ERROR: return "MAX_SIZE_LIMIT_ERROR";
      case PARAMETERS_CHANGED: return "PARAMETERS_CHANGED";
      case POOL_FLUSHED: return "POOL_FLUSHED";
      case POOL_RELEASED: return "POOL_RELEASED";
      default: return "UNKNOWN";
    }
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getName());
    sb.append("[source=");
    sb.append(source.toString());
    sb.append(",type=");
    sb.append(getTypeString());
    sb.append(']');
    return sb.toString();
  }

  public boolean isPoolInitCompleted() { return type == INIT_COMPLETED; }
  public boolean isPoolCheckOut() { return type == CHECKOUT; }
  public boolean isPoolCheckIn() { return type == CHECKIN; }
  public boolean isValidationError() { return type == VALIDATION_ERROR; }
  public boolean isMaxPoolLimitReached() { return type == MAX_POOL_LIMIT_REACHED; }
  public boolean isMaxPoolLimitExceeded() { return type == MAX_POOL_LIMIT_EXCEEDED; }
  public boolean isMaxSizeLimitReached() { return type == MAX_SIZE_LIMIT_REACHED; }
  public boolean isMaxSizeLimitError() { return type == MAX_SIZE_LIMIT_ERROR; }
  public boolean isPoolParametersChanged() { return type == PARAMETERS_CHANGED; }
  public boolean isPoolFlushed() { return type == POOL_FLUSHED; }
  public boolean isPoolReleased() { return type == POOL_RELEASED; }

  void setMinPool(int i) { this.minPool = i; }
  void setMaxPool(int i) { this.maxPool = i; }
  void setMaxSize(int i) { this.maxSize = i; }
  void setIdleTimeout(long i) { this.idleTimeout = i; }
  void setCheckOut(int i) { this.checkedOut = i; }
  void setFreeCount(int i) { this.freeCount = i; }
  void setSize(int i) { this.size = i; }
  void setPoolHitRate(float f) { this.hitRate = f; }

  public int getMinPool() { return minPool; }
  public int getMaxPool() { return maxPool; }
  public int getMaxSize() { return maxSize; }
  public long getIdleTimeout() { return idleTimeout; }
  public int getCheckedOut() { return checkedOut; }
  public int getFreeCount() { return freeCount; }
  public int getSize() { return size; }
  public float getPoolHitRate() { return hitRate; }
}
