package com.groupon.lex.metrics.history.xdr.support;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * A task executor that runs tasks on a ForkJoinPool. The executor ensures not
 * too many tasks are scheduled at once, so it won't starve other jobs running
 * on the same pool.
 *
 * @author ariane
 */
public class FJPTaskExecutor<T> {
    public static final int DEFAULT_CONCURRENCY = Runtime.getRuntime().availableProcessors();
    private final BlockingQueue<Task> done;
    private final Iterator<T> pending;
    private final JobFunction<T> jobFn;

    public FJPTaskExecutor(@NonNull Collection<T> datums, @NonNull JobFunction<T> jobFn, int concurrency) {
        this.done = new ArrayBlockingQueue<>(Integer.max(1, concurrency));
        this.pending = datums.iterator();
        this.jobFn = jobFn;
    }

    public FJPTaskExecutor(@NonNull Collection<T> datums, @NonNull JobFunction<T> jobFn) {
        this(datums, jobFn, DEFAULT_CONCURRENCY);
    }

    public void join() {
        int queued = 0;
        for (int i = done.remainingCapacity(); i > 0 && pending.hasNext(); --i) {
            new Task(pending.next()).fork();
            ++queued;
        }

        while (queued > 0) {
            fjpDone().join();
            --queued;

            if (pending.hasNext()) {
                new Task(pending.next()).fork();
                ++queued;
            }
        }
    }

    public static interface JobFunction<T> {
        public void accept(T v) throws Exception;
    }

    private class FJPDone implements ForkJoinPool.ManagedBlocker {
        private Task doneTask = null;

        public Task get() {
            return doneTask;
        }

        @Override
        public boolean block() throws InterruptedException {
            if (doneTask == null)
                doneTask = done.take();
            return doneTask != null;
        }

        @Override
        public boolean isReleasable() {
            if (doneTask == null)
                doneTask = done.poll();
            return doneTask != null;
        }
    }

    private Task fjpDone() {
        FJPDone blocker = new FJPDone();
        for (;;) {
            try {
                ForkJoinPool.managedBlock(blocker);
                return blocker.get();
            } catch (InterruptedException ex) {
                /* SKIP */
            }
        }
    }

    @RequiredArgsConstructor
    private class Task extends ForkJoinTask<T> {
        private final T argument;
        private T rawResult;

        @Override
        public T getRawResult() {
            return rawResult;
        }

        @Override
        protected void setRawResult(T value) {
            rawResult = value;
        }

        @Override
        protected boolean exec() {
            try {
                jobFn.accept(argument);
                rawResult = argument;
                return true;
            } catch (Error | RuntimeException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new RuntimeException("task execution failed", ex);
            } finally {
                done.add(this);
            }
        }
    }
}
