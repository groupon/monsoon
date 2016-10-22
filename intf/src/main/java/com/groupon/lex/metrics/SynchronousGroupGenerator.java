package com.groupon.lex.metrics;

import java.util.Collection;
import static java.util.Collections.singleton;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public abstract class SynchronousGroupGenerator implements GroupGenerator {
    protected abstract Collection<? extends MetricGroup> getGroups(CompletableFuture<TimeoutObject> timeout);

    @Override
    public final Collection<CompletableFuture<? extends Collection<? extends MetricGroup>>> getGroups(Executor threadpool, CompletableFuture<TimeoutObject> timeout) throws Exception {
        CompletableFuture<Collection<? extends MetricGroup>> result = CompletableFuture.supplyAsync(() -> getGroups(timeout), threadpool);
        timeout.thenAccept(timeoutObject -> {
            result.cancel(true);
        });
        return singleton(result);
    }
}
