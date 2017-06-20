package com.groupon.lex.metrics.history.xdr.support;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

public class Monitor<T, R> implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(Monitor.class.getName());
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final MonitorFunction<T, R> monitorFunction;

    public Monitor(MonitorFunction<T, R> fn) {
        this.monitorFunction = fn;
    }

    public CompletableFuture<R> enqueue(T argument) {
        final MonitorAction newAction = new MonitorAction(argument);
        try {
            try {
                executor.execute(newAction);
            } catch (RejectedExecutionException ex) {
                throw new IllegalStateException("Monitor is closed/closing.", ex);
            }
        } catch (IllegalStateException ex) { // Emit log with stack trace.
            LOG.log(Level.SEVERE, "attempt to enqueue on closed monitor", ex);
            throw ex;
        }
        return newAction.getFuture();
    }

    public CompletableFuture<R> enqueueFuture(CompletableFuture<? extends T> argument) {
        try {
            if (executor.isShutdown())
                throw new IllegalStateException("Monitor is closed/closing.");
        } catch (IllegalStateException ex) { // Emit log with stack trace.
            LOG.log(Level.SEVERE, "attempt to enqueue on closed monitor", ex);
            throw ex;
        }
        return argument.thenCompose(this::enqueue);
    }

    @Override
    public void close() {
        executor.shutdown();
    }

    public static interface MonitorFunction<T, R> {
        public R apply(T v) throws Exception;
    }

    @RequiredArgsConstructor
    private class MonitorAction implements Runnable {
        @Getter
        private final CompletableFuture<R> future = new CompletableFuture<>();
        @NonNull
        private final T argument;

        @Override
        public void run() {
            try {
                future.complete(monitorFunction.apply(argument));
            } catch (Exception | Error ex) {
                future.completeExceptionally(ex);
            }
        }
    }
}
