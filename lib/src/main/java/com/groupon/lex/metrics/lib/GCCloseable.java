/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.groupon.lex.metrics.lib;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import static java.util.Objects.requireNonNull;

/**
 * A wrapper around a closable, that uses an unreachability event by the GC to
 * close the underlying closeable.
 */
public class GCCloseable<T extends AutoCloseable> {
    private static final Logger LOG = Logger.getLogger(GCCloseable.class.getName());
    private static final ReferenceQueue<GCCloseable> CLEANUP_QUEUE = new ReferenceQueue();
    private static final Map<Reference<? extends GCCloseable>, AutoCloseable> INSTANCES = new ConcurrentHashMap<>();
    private static final AtomicBoolean STARTED = new AtomicBoolean(false);

    private static void cleanup_() throws InterruptedException {
        Reference<? extends GCCloseable> ref = CLEANUP_QUEUE.remove();
        final AutoCloseable item = INSTANCES.remove(ref);

        assert (item != null);
        try {
            LOG.log(Level.FINE, "closing {0}", item);
            item.close();
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, "Failed to close " + item, ex);
        }
    }

    private static void ensure_started_() {
        if (STARTED.get()) return;

        synchronized (GCCloseable.class) {
            final Thread t = new Thread(() -> {
                for (;;) {
                    try {
                        cleanup_();
                    } catch (InterruptedException ex) {
                        LOG.log(Level.SEVERE, "ignoring interruption", ex);
                    }
                }
            });
            t.setDaemon(true);
            t.setName(GCCloseable.class.getName() + "-cleaner");
            t.setUncaughtExceptionHandler((Thread thr, Throwable ex) -> {
                LOG.log(Level.WARNING, "uncaught exception", ex);
                STARTED.set(false);
            });

            if (STARTED.compareAndSet(false, true)) {
                LOG.info("starting cleaner thread");
                t.start();
            }
        }
    }

    private final T value_;

    public GCCloseable() {
        value_ = null;
    }

    public GCCloseable(T value) {
        value_ = requireNonNull(value);
        INSTANCES.put(new PhantomReference<>(this, CLEANUP_QUEUE), value_);
        ensure_started_();
    }

    public T get() {
        return value_;
    }
}
