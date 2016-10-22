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
import static java.util.Objects.requireNonNull;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A wrapper around a closable, that uses an unreachability event by the GC to
 * close the underlying closeable.
 */
public class GCCloseable<T extends AutoCloseable> {
    private static final Logger LOG = Logger.getLogger(GCCloseable.class.getName());
    private static final ReferenceQueue<GCCloseable> cleanup_queue_ = new ReferenceQueue();
    private static final Map<Reference<? extends GCCloseable>, AutoCloseable> instances_ = new ConcurrentHashMap<>();
    private static final AtomicBoolean started_ = new AtomicBoolean(false);

    private static void cleanup_() throws InterruptedException {
        Reference<? extends GCCloseable> ref = cleanup_queue_.remove();
        final AutoCloseable item = instances_.remove(ref);

        assert (item != null);
        try {
            LOG.log(Level.FINE, "closing {0}", item);
            item.close();
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, "Failed to close " + item, ex);
        }
    }

    private static void ensure_started_() {
        if (started_.get()) return;

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
            t.setUncaughtExceptionHandler((Thread thr, Throwable ex) -> Logger.getLogger(GCCloseable.class.getName()).log(Level.WARNING, "uncaught exception", ex));

            if (started_.compareAndSet(false, true)) {
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
        instances_.put(new PhantomReference<>(this, cleanup_queue_), value_);
        ensure_started_();
    }

    public T get() {
        return value_;
    }
}
