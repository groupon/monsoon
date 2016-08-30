package com.groupon.lex.metrics.resolver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import static java.util.Collections.unmodifiableList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;

/**
 * Resolver that simply returns a set of tuples it was initialized with.
 */
@Getter
public class ConstResolver implements Resolver {
    private final List<ResolverTuple> tuples;
    private final int tupleWidth;

    public ConstResolver(Collection<ResolverTuple> values) {
        this.tuples = unmodifiableList(new ArrayList<>(values));

        // Derive tuple width.
        if (this.tuples.isEmpty()) {
            tupleWidth = Integer.MAX_VALUE;
        } else {
            final Iterator<ResolverTuple> iter = this.tuples.iterator();
            tupleWidth = iter.next().getFieldsSize();
            // Validate tuple width is the same across the entire collection.
            iter.forEachRemaining(t -> {
                if (t.getFieldsSize() != tupleWidth)
                    throw new IllegalArgumentException("Tuple width mismatch");
            });
        }
    }

    public ConstResolver(ResolverTuple... values) {
        this(Arrays.asList(values));
    }

    @Override
    public String configString() {
        if (tuples.isEmpty()) return "[]";
        return tuples.stream()
                .map(ResolverTuple::toString)
                .collect(Collectors.joining(", ", "[ ", " ]"));
    }
}
