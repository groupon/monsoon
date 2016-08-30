package com.groupon.lex.metrics.resolver;

import com.groupon.lex.metrics.lib.Any3;
import static com.groupon.lex.metrics.resolver.ResolverTuple.newTupleElement;
import java.util.Arrays;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class ResolverTupleTest {
    @Test
    public void new_tuple_element() {
        assertEquals(Any3.create1(false), ResolverTuple.newTupleElement(false));
        assertEquals(Any3.create1(true), ResolverTuple.newTupleElement(true));
        assertEquals(Any3.create2(19), ResolverTuple.newTupleElement(19));
        assertEquals(Any3.create3("TEST"), ResolverTuple.newTupleElement("TEST"));
    }

    @Test
    public void new_tuple() {
        ResolverTuple rt = new ResolverTuple(newTupleElement(false), newTupleElement(17), newTupleElement("bar"));

        assertEquals(3, rt.getFieldsSize());
        assertEquals(rt.getFields(), Arrays.asList(newTupleElement(false), newTupleElement(17), newTupleElement("bar")));
    }

    @Test
    public void equality() {
        ResolverTuple rt_orig = new ResolverTuple(newTupleElement(false), newTupleElement(17), newTupleElement("bar"));
        ResolverTuple rt_same = new ResolverTuple(newTupleElement(false), newTupleElement(17), newTupleElement("bar"));

        assertEquals(rt_orig.hashCode(), rt_same.hashCode());
        assertTrue(rt_orig.equals(rt_same));
    }

    @Test
    public void inequality() {
        ResolverTuple rt_orig = new ResolverTuple(newTupleElement(false), newTupleElement(17), newTupleElement("bar"));
        ResolverTuple rt_diff_1 = new ResolverTuple(newTupleElement(true), newTupleElement(17), newTupleElement("bar"));
        ResolverTuple rt_diff_2 = new ResolverTuple(newTupleElement(false), newTupleElement(16), newTupleElement("bar"));
        ResolverTuple rt_diff_3 = new ResolverTuple(newTupleElement(false), newTupleElement(17), newTupleElement("baz"));

        assertFalse(rt_orig.equals(rt_diff_1));
        assertFalse(rt_orig.equals(rt_diff_2));
        assertFalse(rt_orig.equals(rt_diff_3));
    }

    @Test
    public void to_string() {
        assertEquals("()", new ResolverTuple().toString());
        assertEquals("false", new ResolverTuple(newTupleElement(false)).toString());
        assertEquals("true", new ResolverTuple(newTupleElement(true)).toString());
        assertEquals("17", new ResolverTuple(newTupleElement(17)).toString());
        assertEquals("\"foobar\"", new ResolverTuple(newTupleElement("foobar")).toString());
        assertEquals("(\"a\", \"b\")", new ResolverTuple(newTupleElement("a"), newTupleElement("b")).toString());
    }
}
