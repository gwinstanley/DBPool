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

import java.util.ArrayList;
import java.util.EventListener;
import java.util.EventObject;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Utility class to perform asynchronous event dispatch.
 * it provides a simple mechanism to allow dispatch of events using the
 * single method call {@link #dispatchEvent(EventObject)}, which schedules the event
 * to be propagated to the registered listeners in a separate thread.
 * The mechanism of actual event notification is specified using an
 * implementation of {@link EventNotifier}.
 * <p>The {@link EventListener} list provided on creation should ideally
 * be an instance of {@link CopyOnWriteArrayList}, which allows safe iteration
 * without making a copy. If this type is not used, a copy of of the listeners
 * is taken each time an event dispatch is performed, synchronizing on the list
 * instance during the copy.</p>
 *
 * @author Giles Winstanley
 * @param <L> class type of event listener
 * @param <E> class type of event object
 */
public class EventDispatcher<L extends EventListener, E extends EventObject> extends Thread
{
  /** Event listeners to which to dispatch events. */
  private final List<L> listeners;
  /** Instance to use to publish events to listeners. */
  private final EventNotifier<L,E> notifier;
  /** Queue of events to dispatch. */
  private final Queue<E> events = new ConcurrentLinkedQueue<>();
  /** Flag determining whether the cleaner has been stopped. */
  private volatile boolean stopped = false;

  public EventDispatcher(List<L> listeners, EventNotifier<L,E> notifier)
  {
    Objects.requireNonNull(listeners);
    Objects.requireNonNull(notifier);
    this.listeners = listeners;
    this.notifier = notifier;
    this.setDaemon(true);
  }

  @Override
  public void start()
  {
    stopped = false;
    super.start();
  }

  /**
   * Halts this thread (use instead of {@link #stop()}).
   */
  public void halt()
  {
    stopped = true;
    this.interrupt();
  }

  /**
   * Schedules the specified event for listener notification.
   * @param event event to dispatch
   */
  public void dispatchEvent(E event)
  {
    if (event == null)
      return;
    events.add(event);
    synchronized (this)
    {
      this.notify();
    }
  }

  @Override
  public void run()
  {
    while (!stopped)
    {
      if (events.isEmpty())
      {
        synchronized (this)
        {
          try { this.wait(); }
          catch (InterruptedException ix) {}  // Interruption ignored.
        }
      }
      else
      {
        // Get next event to propagate.
        E event = events.poll();
        if (event == null)
          continue;

        // Check if a copy needs to be taken before iterating over listeners.
        List<L> temp = null;
        boolean safe = (listeners instanceof CopyOnWriteArrayList);
        if (safe)
          temp = listeners;
        else
          synchronized(listeners) { temp = new ArrayList<>(listeners); }
        // Iterate over listeners to notify them of event.
        for (L listener : temp)
        {
          try
          {
            // Notify each listener of event.
            notifier.notifyListener(listener, event);
          }
          catch (RuntimeException rx)
          {
            rx.printStackTrace();
          }
        }
      }
    }
  }
}
