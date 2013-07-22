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

import java.util.EventListener;

/**
 * Listener interface for {@link ObjectPoolEvent} objects.
 * Listeners should ensure the implementations of the listed methods return
 * quickly. Tasks that require more time should spawn a new thread.
 * 
 * @author Giles Winstanley
 */
public interface ObjectPoolListener extends EventListener
{
  /**
   * Called when the pool's {@link ObjectPool#init(int)} method has completed.
   */
  public void poolInitCompleted(ObjectPoolEvent evt);
  /**
   * Called when an item is checked out of the pool.
   */
  public void poolCheckOut(ObjectPoolEvent evt);
  /**
   * Called when an item is checked back in to the pool.
   */
  public void poolCheckIn(ObjectPoolEvent evt);
  /**
   * Called when an item is found to be invalid.
   */
  public void validationError(ObjectPoolEvent evt);
  /**
   * Called when a check-out request causes the poolSize limit to be reached.
   */
  public void maxPoolLimitReached(ObjectPoolEvent evt);
  /**
   * Called when a check-out request causes the poolSize limit to be exceeded.
   */
  public void maxPoolLimitExceeded(ObjectPoolEvent evt);
  /**
   * Called when a check-out request causes the maxSize limit to be reached.
   */
  public void maxSizeLimitReached(ObjectPoolEvent evt);
  /**
   * Called when a check-out request attempts to exceed the maxSize limit.
   */
  public void maxSizeLimitError(ObjectPoolEvent evt);
  /**
   * Called when the pool's parameters are changed.
   */
  public void poolParametersChanged(ObjectPoolEvent evt);
  /**
   * Called when the pool is flushed of all free/unused items.
   */
  public void poolFlushed(ObjectPoolEvent evt);
  /**
   * Called when the pool is released (no more events are fired by the pool after this event).
   */
  public void poolReleased(ObjectPoolEvent evt);
}
