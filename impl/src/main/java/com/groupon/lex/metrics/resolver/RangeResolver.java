package com.groupon.lex.metrics.resolver;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
        return IntStream.range(begin, end)
                .mapToObj(ResolverTuple::newTupleElement)
                .map(ResolverTuple::new)
                .collect(Collectors.toList());
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
