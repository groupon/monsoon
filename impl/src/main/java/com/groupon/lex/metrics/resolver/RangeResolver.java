package com.groupon.lex.metrics.resolver;

import static com.groupon.lex.metrics.resolver.ResolverTuple.newTupleElement;
import static java.lang.Integer.max;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * A resolver that emits a range of numbers.
 */
@Getter
@AllArgsConstructor
public class RangeResolver implements Resolver {
    private final int begin, end;

    public RangeResolver(int end) {
        this(0, end);
    }

    @Override
    public int getTupleWidth() {
        return 1;
    }

    @Override
    public Collection<ResolverTuple> getTuples() {
        List<ResolverTuple> tuples = new ArrayList<>(max(end - begin, 0));
        for (int i = begin; i < end; ++i)
            tuples.add(new ResolverTuple(newTupleElement(i)));
        return tuples;
    }

    @Override
    public String configString() {
        StringBuilder buf = new StringBuilder()
                .append("range(");
        if (begin != 0) {
            buf
                    .append(begin)
                    .append(", ");
        }
        return buf
                .append(end)
                .append(')')
                .toString();
    }
}
