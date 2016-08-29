package com.groupon.lex.metrics.resolver;

import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.lib.Any3;
import static com.groupon.lex.metrics.resolver.ResolverTuple.newTupleElement;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.EMPTY_MAP;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import java.util.HashMap;
import java.util.stream.Collectors;
import org.hamcrest.Matchers;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class BoundNameResolverTest {
    @Test
    public void empty() throws Exception {
        BoundNameResolver bnr = new BoundNameResolver(new BoundNameResolver.Names(), new ConstResolver(new ResolverTuple()));

        assertEquals("() = " + bnr.getResolver().configString(), bnr.configString());
        assertTrue(bnr.isEmpty());
        assertEquals(EMPTY_LIST, bnr.getKeys().collect(Collectors.toList()));
        assertEquals(singletonList(EMPTY_MAP), bnr.resolve().collect(Collectors.toList()));
    }

    @Test
    public void resolve() throws Exception {
        BoundNameResolver bnr = new BoundNameResolver(
                new BoundNameResolver.Names(Any2.right("foo")),
                new ConstResolver(
                        new ResolverTuple(newTupleElement("one")),
                        new ResolverTuple(newTupleElement("two"))));

        assertThat(bnr.resolve().collect(Collectors.toList()),
                Matchers.containsInAnyOrder(
                        singletonMap(Any2.right("foo"), Any3.create3("one")),
                        singletonMap(Any2.right("foo"), Any3.create3("two"))));
        assertEquals("foo = " + bnr.getResolver().configString(), bnr.configString());
    }

    @Test
    public void resolve_paired() throws Exception {
        BoundNameResolver bnr = new BoundNameResolver(
                new BoundNameResolver.Names(Any2.left(0), Any2.left(1)),
                new ConstResolver(
                        new ResolverTuple(newTupleElement("x"), newTupleElement("y")),
                        new ResolverTuple(newTupleElement("a"), newTupleElement("b"))));

        assertThat(bnr.resolve().collect(Collectors.toList()),
                Matchers.containsInAnyOrder(
                        new HashMap<Any2<Integer, String>, Any3<Boolean, Integer, String>>() {{
                            put(Any2.left(0), Any3.create3("x"));
                            put(Any2.left(1), Any3.create3("y"));
                        }},
                        new HashMap<Any2<Integer, String>, Any3<Boolean, Integer, String>>() {{
                            put(Any2.left(0), Any3.create3("a"));
                            put(Any2.left(1), Any3.create3("b"));
                        }}));
        assertEquals("(0, 1) = " + bnr.getResolver().configString(), bnr.configString());
    }

    @Test
    public void accept_shorter_names() throws Exception {
        BoundNameResolver bnr = new BoundNameResolver(
                new BoundNameResolver.Names(Any2.left(0)),
                new ConstResolver(
                        new ResolverTuple(newTupleElement("x"), newTupleElement("y")),
                        new ResolverTuple(newTupleElement("a"), newTupleElement("b"))));

        assertThat(bnr.resolve().collect(Collectors.toList()),
                Matchers.containsInAnyOrder(
                        new HashMap<Any2<Integer, String>, Any3<Boolean, Integer, String>>() {{
                            put(Any2.left(0), Any3.create3("x"));
                        }},
                        new HashMap<Any2<Integer, String>, Any3<Boolean, Integer, String>>() {{
                            put(Any2.left(0), Any3.create3("a"));
                        }}));
        assertEquals("0 = " + bnr.getResolver().configString(), bnr.configString());
    }

    @Test
    public void correct_escaping() throws Exception {
        BoundNameResolver bnr = new BoundNameResolver(
                new BoundNameResolver.Names(Any2.right("match")),
                new ConstResolver());

        assertEquals("'match' = " + bnr.getResolver().configString(), bnr.configString());
    }
}
