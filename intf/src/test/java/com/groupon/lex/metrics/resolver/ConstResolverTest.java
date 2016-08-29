package com.groupon.lex.metrics.resolver;

import static com.groupon.lex.metrics.resolver.ResolverTuple.newTupleElement;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class ConstResolverTest {
    @Test
    public void pair_of_pairs() {
        ConstResolver cr = new ConstResolver(
                new ResolverTuple(newTupleElement(0), newTupleElement("zero")),
                new ResolverTuple(newTupleElement(1), newTupleElement("one")));

        assertEquals("[ (0, \"zero\"), (1, \"one\") ]", cr.configString());
        assertEquals(2, cr.getTupleWidth());
        assertThat(cr.getTuples(),
                containsInAnyOrder(
                        new ResolverTuple(newTupleElement(0), newTupleElement("zero")),
                        new ResolverTuple(newTupleElement(1), newTupleElement("one"))));
    }

    @Test(expected = IllegalArgumentException.class)
    public void disallow_mismatched_tuple_length() {
        new ConstResolver(
                new ResolverTuple(newTupleElement("one element")),
                new ResolverTuple(newTupleElement("two"), newTupleElement("elements")));
    }

    @Test
    public void empty_tuple() {
        ConstResolver cr = new ConstResolver();

        assertEquals("[]", cr.configString());
        assertEquals(Integer.MAX_VALUE, cr.getTupleWidth());
        assertTrue(cr.getTuples().isEmpty());
    }
}
