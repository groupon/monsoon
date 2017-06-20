package com.groupon.lex.metrics.history.xdr.support;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

public class Monitor<T, R> implements AutoCloseable {
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final MonitorFunction<T, R> monitorFunction;

    public Monitor(MonitorFunction<T, R> fn) {
        this.monitorFunction = fn;
    }

    public CompletableFuture<R> enqueue(T argument) {
        final MonitorAction newAction = new MonitorAction(argument);
        try {
            executor.execute(newAction);
        } catch (RejectedExecutionException ex) {
            throw new IllegalStateException("Monitor is closed/closing.", ex);
        }
        return newAction.getFuture();
    }

    public CompletableFuture<R> enqueueFuture(CompletableFuture<? extends T> argument) {
        if (executor.isShutdown())
            throw new IllegalStateException("Monitor is closed/closing.");
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
