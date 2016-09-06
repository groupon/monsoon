package com.groupon.lex.metrics.resolver;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;

/**
 * A resolver, used in import statements to generate data on the fly.
 *
 * Resolvers return tuples of a fixed length.
 */
public interface AsyncResolver extends AutoCloseable {
    /** Returns the number of elements per tuple. */
    public int getTupleWidth();
    /** Resolves tuples. */
    public CompletableFuture<? extends Collection<ResolverTuple>> getTuples() throws Exception;

    public String configString();
    @Override
    public default void close() throws Exception {}

    public static AsyncResolver makeAsync(@NonNull Resolver resolver) {
        return new AsyncResolver() {
            @Override
            public int getTupleWidth() { return resolver.getTupleWidth(); }
            @Override
            public String configString() { return resolver.configString(); }

            @Override
            public CompletableFuture<Collection<ResolverTuple>> getTuples() {
                try {
                    return CompletableFuture.completedFuture(resolver.getTuples());
                } catch (Exception ex) {
                    final CompletableFuture<Collection<ResolverTuple>> failFut = new CompletableFuture<>();
                    failFut.completeExceptionally(ex);
                    return failFut;
                }
            }

            @Override
            public void close() throws Exception { resolver.close(); }
        };
    }
}
