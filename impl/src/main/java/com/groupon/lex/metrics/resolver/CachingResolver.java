/*
 * Copyright (c) 2016, Groupon, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * Neither the name of GROUPON nor the names of its contributors may be
 * used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.groupon.lex.metrics.resolver;

import static com.groupon.lex.metrics.ConfigSupport.durationConfigString;
import java.util.Collection;
import static java.util.Collections.EMPTY_LIST;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.NonNull;
import org.joda.time.DateTime;
import org.joda.time.Duration;

public class CachingResolver implements Resolver {
    private static enum Refresh {
        REUSE,
        MAY_REFRESH,
        MUST_REFRESH
    }

    private static final Logger LOG = Logger.getLogger(CachingResolver.class.getName());
    /** Scheduler used for async resolution. */
    private static final ScheduledExecutorService SCHEDULER;
    /** Time (msec) until next attempt to reload tuples, after a failed reload attempt. */
    private static final int FAILURE_RESCHEDULE_DELAY = 5000;
    private final AsyncResolver underlying;
    private final Duration minAge, maxAge;
    private DateTime lastRefresh;
    private Collection<ResolverTuple> tuples = EMPTY_LIST;
    private Throwable exception = null;
    private CompletableFuture<?> currentFut = null;
    private boolean closed = false;

    static {
        final AtomicInteger thrIndex = new AtomicInteger();

        final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1,
                (Runnable r) -> {
                    final Thread thr = new Thread(r);
                    thr.setName("CachingResolver-worker-" + thrIndex.getAndIncrement());
                    thr.setDaemon(true);
                    return thr;
                });
        try {
            scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            scheduler.setKeepAliveTime(5, TimeUnit.MINUTES);
            scheduler.setMaximumPoolSize(4);
            SCHEDULER = scheduler;
        } catch (RuntimeException ex) {
            scheduler.shutdown();
            throw ex;
        }
    }

    public CachingResolver(@NonNull AsyncResolver underlying, @NonNull Duration minAge, @NonNull Duration maxAge) {
        this.underlying = underlying;
        this.minAge = minAge;
        this.maxAge = maxAge;
        this.lastRefresh = null;

        refresh();
    }

    public CachingResolver(@NonNull Resolver underlying, @NonNull Duration minAge, @NonNull Duration maxAge) {
        this(AsyncResolver.makeAsync(underlying), minAge, maxAge);
    }

    @Override
    public int getTupleWidth() {
        return underlying.getTupleWidth();
    }

    @Override
    public synchronized Collection<ResolverTuple> getTuples() throws Exception {
        if (closed) throw new IllegalStateException("resolver is closed");

        final Refresh refresh = getRefresh();
        Collection<ResolverTuple> result;
        switch (refresh) {
            case REUSE:
            case MAY_REFRESH:
                result = tuples;
                break;
            case MUST_REFRESH:
                if (exception == null)
                    throw new StaleResolverException();
                else
                    throw new StaleResolverException(exception);
            default:
                assert(false);
                throw new IllegalStateException("unreachable statement");
        }
        return result;
    }

    @Override
    public String configString() {
        return underlying.configString() + "[" + durationConfigString(minAge) + " - " + durationConfigString(maxAge) + " ]";
    }

    @Override
    public synchronized void close() {
        closed = true;
        if (currentFut != null) {
            currentFut.cancel(false);
            currentFut = null;
        }

        // Closing underlying is done by reschedule loop.
    }

    private synchronized Refresh getRefresh() {
        if (lastRefresh == null) return Refresh.MUST_REFRESH;
        final DateTime mayTP = lastRefresh.plus(minAge), mustTP = lastRefresh.plus(maxAge);
        final DateTime now = DateTime.now();

        if (now.isAfter(mustTP)) return Refresh.MUST_REFRESH;
        if (now.isAfter(mayTP)) return Refresh.MAY_REFRESH;
        return Refresh.REUSE;
    }

    /**
     * Attempt to refresh the resolver result.
     * @return a boolean, with true indicating success and false indicating failure.
     */
    private void refresh() {
        try {
            // Invoke underlying getTuples unsynchronized.
            // This way it won't block tuple access, in case it is a long
            // running or expensive function.
            final CompletableFuture<? extends Collection<ResolverTuple>> fut = underlying.getTuples();

            // Publish future, needs synchronization, so close() method can cancel properly.
            synchronized(this) {
                // We create the whenCompleteAsync continuation with synchronization locked,
                // because the update code will want to clear the currentFut member-variable.
                // We must ensure it is set before.
                currentFut = fut.whenCompleteAsync(this::update, SCHEDULER);

                if (closed)  // Close may have set close bit while we were creating future.
                    fut.cancel(false);
            }
        } catch (Exception ex) {
            synchronized(this) {
                exception = ex;
            }
            reschedule(FAILURE_RESCHEDULE_DELAY);
        }
    }

    /** Update callback, called when the completable future completes. */
    private void update(Collection<ResolverTuple> underlyingTuples, Throwable t) {
        if (t != null)
            applyException(t);
        else if (underlyingTuples != null)
            applyUpdate(underlyingTuples);
        else
            applyException(null);
    }

    /** Apply updated tuple collection. */
    private synchronized void applyUpdate(Collection<ResolverTuple> underlyingTuples) {
        lastRefresh = DateTime.now();
        tuples = underlyingTuples;
        exception = null;
        reschedule(minAge.getMillis());
    }

    /**
     * Update the exception of the last refresh attempt.
     * The last refresh exception is used as a cause for StaleException.
     */
    private synchronized void applyException(Throwable t) {
        exception = t;
        reschedule(FAILURE_RESCHEDULE_DELAY);
    }

    /** Schedule next invocation of the update task. */
    private synchronized void reschedule(long millis) {
        currentFut = null;  // Remove current future.

        if (!closed) {
            SCHEDULER.schedule(this::refresh, millis, TimeUnit.MILLISECONDS);
        } else {
            try {
                underlying.close();
            } catch (Exception ex) {
                LOG.log(Level.WARNING, "failed to close resolver " + underlying.configString(), ex);
            }
        }
    }

    public static class StaleResolverException extends Exception {
        public StaleResolverException() { super("stale resolver"); }
        public StaleResolverException(Throwable cause) { super("stale resolver", cause); }
    }
}
