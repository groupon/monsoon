package com.groupon.lex.metrics.resolver;

import static com.groupon.lex.metrics.resolver.ResolverTuple.newTupleElement;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import org.junit.Test;

public class RangeResolverTest {
    @Test
    public void constructedWithoutBegin() {
        RangeResolver rr = new RangeResolver(3);

        assertEquals("range(3)", rr.configString());
        assertEquals(0, rr.getBegin());
        assertEquals(3, rr.getEnd());
        assertEquals(1, rr.getTupleWidth());
        assertThat(rr.getTuples(), containsInAnyOrder(
                new ResolverTuple(newTupleElement(0)),
                new ResolverTuple(newTupleElement(1)),
                new ResolverTuple(newTupleElement(2))
        ));
    }

    @Test
    public void constructedWithBegin() {
        RangeResolver rr = new RangeResolver(3, 6);

        assertEquals("range(3, 6)", rr.configString());
        assertEquals(3, rr.getBegin());
        assertEquals(6, rr.getEnd());
        assertEquals(1, rr.getTupleWidth());
        assertThat(rr.getTuples(), containsInAnyOrder(
                new ResolverTuple(newTupleElement(3)),
                new ResolverTuple(newTupleElement(4)),
                new ResolverTuple(newTupleElement(5))
        ));
    }
}
