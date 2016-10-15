package com.groupon.lex.metrics.history.xdr.support;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Future;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

public class Monitor<T, R> implements AutoCloseable {
    private final Queue<ForkJoinTask<R>> tasks = new ArrayDeque<>();
    private final ForkJoinPool pool = decideOnAPool();
    private final MonitorFunction<T, R> monitorFunction;
    private boolean active = false;
    private boolean closed = false;

    public Monitor(MonitorFunction<T, R> fn) {
        this.monitorFunction = fn;
    }

    private static ForkJoinPool decideOnAPool() {
        final ForkJoinPool currentPool = ForkJoinTask.getPool();
        if (currentPool != null)
            return currentPool;
        return ForkJoinPool.commonPool();
    }

    public synchronized Future<R> enqueue(T argument) {
        if (closed)
            throw new IllegalStateException("Monitor is closed/closing.");

        MonitorAction newAction = new MonitorAction(argument);
        tasks.add(newAction);
        fire();
        return newAction;
    }

    private synchronized void fire() {
        if (!active) {
            ForkJoinTask<R> head = tasks.poll();
            if (head != null) {
                pool.submit(head);
                active = true;
            }
        }
    }

    @Override
    public void close() {
        final ForkJoinTask<R> sentinel;
        synchronized (this) {
            if (closed)
                return;
            sentinel = ForkJoinTask.adapt(() -> {
            }, null);
            tasks.add(sentinel);
            fire();
            closed = true;
        }
        sentinel.join();
    }

    public static interface MonitorFunction<T, R> {
        public R apply(T v) throws Exception;
    }

    private class MonitorAction extends ForkJoinTask<R> {
        private T argument;

        @Getter(AccessLevel.PUBLIC)
        @Setter(AccessLevel.PROTECTED)
        private R rawResult = null;

        public MonitorAction(T argument) {
            this.argument = argument;
        }

        @Override
        protected boolean exec() {
            try {
                setRawResult(monitorFunction.apply(argument));
                return true;
            } catch (Error | RuntimeException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new MonitorException(ex);
            } finally {
                argument = null;  // Release resources.
                synchronized (Monitor.this) {
                    active = false;
                    fire();
                }
            }
        }
    }

    public static class MonitorException extends RuntimeException {
        public MonitorException(Throwable cause) {
            super(cause);
        }
    }
}
