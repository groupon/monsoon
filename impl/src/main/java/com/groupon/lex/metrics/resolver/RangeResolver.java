package com.groupon.lex.metrics.resolver;

import static com.groupon.lex.metrics.resolver.ResolverTuple.newTupleElement;
import static java.lang.Integer.max;
import java.util.ArrayList;
import static java.util.Collections.unmodifiableList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * A resolver that emits a range of numbers.
 *
 * Bounds are inclusive.
 */
@Getter
@ToString
@EqualsAndHashCode
public class RangeResolver implements Resolver {
    private final int begin, end;
    private final List<ResolverTuple> tuples;

    public RangeResolver(int begin, int end) {
        this.begin = begin;
        this.end = end;
        this.tuples = createTuples(begin, end);
    }

    @Override
    public int getTupleWidth() { return 1; }
    @Override
    public String configString() { return begin + ".." + end; }

    private static List<ResolverTuple> createTuples(int begin, int end) {
        final ArrayList<ResolverTuple> tuples = new ArrayList<>(max(end - begin + 1, 0));

        /*
         * We need a for-loop: for (int i = begin; i <= end; ++i) { ... }
         * But this won't work if end == Integer.MAX_VALUE, since i can never
         * exceed Integer.MAX_VALUE (wrapping around to Integer.MIN_VALUE
         * instead).
         * So the equals-case is handled separately in an if-statement below.
         */
        for (int i = begin; i < end; ++i)
            tuples.add(new ResolverTuple(newTupleElement(i)));
        if (end >= begin)
            tuples.add(new ResolverTuple(newTupleElement(end)));

        return unmodifiableList(tuples);
    }
}
