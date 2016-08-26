package com.groupon.lex.metrics.resolver;

import java.util.Collection;

/**
 * A resolver, used in import statements to generate data on the fly.
 *
 * Resolvers return tuples of a fixed length.
 */
public interface Resolver {
    /** Returns the number of elements per tuple. */
    public int getTupleWidth();
    /** Resolves tuples. */
    public Collection<ResolverTuple> getTuples() throws Exception;

    public String configString();
}
