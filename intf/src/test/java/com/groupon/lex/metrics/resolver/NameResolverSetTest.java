package com.groupon.lex.metrics.resolver;

import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.lib.Any3;
import static com.groupon.lex.metrics.resolver.ResolverTuple.newTupleElement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Test;

public class NameResolverSetTest {
    public final static NameResolver AB = new BoundNameResolver(
            new BoundNameResolver.Names(Any2.left(0)),
            new ConstResolver(
                    new ResolverTuple(newTupleElement("a")),
                    new ResolverTuple(newTupleElement("b"))));

    public final static NameResolver XY = new BoundNameResolver(
            new BoundNameResolver.Names(Any2.left(1)),
            new ConstResolver(
                    new ResolverTuple(newTupleElement("x")),
                    new ResolverTuple(newTupleElement("y"))));
    private NameResolverSet nrs;

    @Before
    public void setup() {
        nrs = new NameResolverSet(AB, XY);
    }

    @Test
    public void config_string() throws Exception {
        assertEquals(
                "{\n"
              + "    " + AB.configString() + ",\n"
              + "    " + XY.configString() + "\n"
              + "}",
                nrs.configString());
    }

    @Test
    public void product() throws Exception {
        final List<Map<Any2<Integer, String>, Any3<Boolean, Integer, String>>> resolution = nrs.resolve().collect(Collectors.toList());

        assertThat(resolution,
                containsInAnyOrder(
                        new HashMap<Any2<Integer, String>, Any3<Boolean, Integer, String>>() {{
                            put(Any2.left(0), Any3.create3("a"));
                            put(Any2.left(1), Any3.create3("x"));
                        }},
                        new HashMap<Any2<Integer, String>, Any3<Boolean, Integer, String>>() {{
                            put(Any2.left(0), Any3.create3("a"));
                            put(Any2.left(1), Any3.create3("y"));
                        }},
                        new HashMap<Any2<Integer, String>, Any3<Boolean, Integer, String>>() {{
                            put(Any2.left(0), Any3.create3("b"));
                            put(Any2.left(1), Any3.create3("x"));
                        }},
                        new HashMap<Any2<Integer, String>, Any3<Boolean, Integer, String>>() {{
                            put(Any2.left(0), Any3.create3("b"));
                            put(Any2.left(1), Any3.create3("y"));
                        }}
                ));
    }

    @Test
    public void keys() throws Exception {
        final List<Any2<Integer, String>> keys = nrs.getKeys().collect(Collectors.toList());

        assertThat(keys,
                containsInAnyOrder(
                        Any2.left(0),
                        Any2.left(1)
                ));
    }

    @Test
    public void not_empty() {
        assertFalse(nrs.isEmpty());
    }
}
