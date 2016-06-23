/*
 * Copyright (c) 2016, Groupon, Inc.
 * All rights reserved. 
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met: 
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer. 
 *
 * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution. 
 *
 * Neither the name of GROUPON nor the names of its contributors may be
 * used to endorse or promote products derived from this software without
 * specific prior written permission. 
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.groupon.lex.metrics;

import java.util.Collection;
import static java.util.Collections.EMPTY_MAP;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class TagsTest {
    @Test
    public void empty_map() {
        final Tags tags = new Tags(EMPTY_MAP);

        assertTrue(tags.asMap().isEmpty());
        assertTrue(tags.isEmpty());
        assertEquals(Optional.empty(), tags.getTag("foo"));
        assertEquals("", tags.getTagString());
        assertEquals("{}", tags.toString());
    }

    @Test
    public void empty_stream() {
        final Tags tags = new Tags(Stream.empty());

        assertTrue(tags.asMap().isEmpty());
        assertTrue(tags.isEmpty());
        assertEquals(Optional.empty(), tags.getTag("foo"));
        assertEquals("", tags.getTagString());
        assertEquals("{}", tags.toString());
    }

    @Test
    public void from_stream() {
        final Tags tags = new Tags(singletonMap("foo", MetricValue.fromStrValue("bar")).entrySet().stream());

        assertEquals(new Tags(singletonMap("foo", MetricValue.fromStrValue("bar"))), tags);
    }

    @Test(expected = IllegalArgumentException.class)
    public void from_bad_stream() {
        new Tags(singletonMap("foo", MetricValue.EMPTY).entrySet().stream());
    }

    @Test(expected = IllegalStateException.class)
    public void from_duplicates_stream() {
        final Set<Map.Entry<String, MetricValue>> elems = singletonMap("foo", MetricValue.fromStrValue("bar")).entrySet();

        new Tags(Stream.concat(elems.stream(), elems.stream()));
    }

    @Test
    public void present() {
        final Tags tags = new Tags(singletonMap("foo", MetricValue.fromStrValue("bar")));

        assertThat(tags.asMap(), hasEntry("foo", MetricValue.fromStrValue("bar")));
        assertFalse(tags.isEmpty());
        assertEquals(Optional.of(MetricValue.fromStrValue("bar")), tags.getTag("foo"));
        assertEquals("foo=\"bar\"", tags.getTagString());
        assertEquals("{foo=\"bar\"}", tags.toString());
    }

    @Test
    public void equality() {
        final Tags t_empty = new Tags(EMPTY_MAP);
        final Tags t_foo_1 = new Tags(singletonMap("foo", MetricValue.fromStrValue("bar")));
        final Tags t_foo_2 = new Tags(singletonMap("foo", MetricValue.fromStrValue("bar")));

        assertEquals(Tags.EMPTY, t_empty);
        assertEquals(t_foo_1, t_foo_2);
        assertNotEquals(t_empty, t_foo_1);
        assertNotEquals(t_foo_1, t_empty);

        assertEquals(0, Tags.EMPTY.compareTo(t_empty));
        assertEquals(0, t_foo_1.compareTo(t_foo_2));
    }

    @Test
    public void inequality_type() {
        final Tags tags = new Tags(singletonMap("foo", MetricValue.fromStrValue("bar")));

        assertFalse(tags.equals(null));
        assertFalse(tags.equals(new Object()));
    }

    @Test
    public void iterator() {
        final Tags tags = new Tags(singletonMap("foo", MetricValue.fromStrValue("bar")));

        assertFalse(Tags.EMPTY.iterator().hasNext());
        assertTrue(tags.iterator().hasNext());
        assertThat(tags.iterator().next(), notNullValue());
    }

    @Test
    public void hascode_changes_with_contents() {
        final Tags t0 = new Tags(singletonMap("fizz", MetricValue.fromStrValue("bar")));
        final Tags t1 = new Tags(singletonMap("buzz", MetricValue.fromStrValue("bar")));
        final Tags t2 = new Tags(singletonMap("fizz", MetricValue.fromStrValue("bar")));

        if (t0.asMap().hashCode() != t1.asMap().hashCode())
            assertNotEquals(t0.hashCode(), t1.hashCode());
        assertEquals(t0.hashCode(), t2.hashCode());
    }

    @Test
    public void filter() {
        final Tags tags = new Tags(singletonMap("fizz", MetricValue.fromStrValue("bar")));

        assertThat(tags.filter(singleton("fizz")).asMap(), hasEntry("fizz", MetricValue.fromStrValue("bar")));
        assertThat(tags.filter(singleton("buzz")).asMap(), not(hasEntry("fizz", MetricValue.fromStrValue("bar"))));
    }

    @Test
    public void tag_as_string() {
        final Tags tags = new Tags(new HashMap<String, MetricValue>() {{
            put("bool", MetricValue.TRUE);
            put("int", MetricValue.fromIntValue(17));
            put("flt", MetricValue.fromDblValue(19));
            put("str", MetricValue.fromStrValue("foobarium"));
        }});

        assertEquals("bool=true, flt=" + Double.toString(19) + ", int=17, str=\"foobarium\"",
            tags.getTagString());
    }

    @Test
    public void compare() {
        final Tags t0 = new Tags(singletonMap("a", MetricValue.TRUE));
        final Tags t1 = new Tags(singletonMap("b", MetricValue.fromIntValue(7)));
        final Tags t2 = new Tags(singletonMap("b", MetricValue.fromIntValue(9)));
        final Tags t3 = new Tags(
                Stream.of(singletonMap("b", MetricValue.fromIntValue(9)),
                                singletonMap("c", MetricValue.fromIntValue(9)))
                        .map(Map::entrySet)
                        .flatMap(Collection::stream));
        final Tags t4 = new Tags(singletonMap("c", MetricValue.fromIntValue(9)));

        assertTrue(t0.compareTo(t1) < 0);
        assertTrue(t0.compareTo(t2) < 0);
        assertTrue(t0.compareTo(t3) < 0);
        assertTrue(t0.compareTo(t4) < 0);

        assertTrue(t1.compareTo(t0) > 0);
        assertTrue(t1.compareTo(t2) < 0);
        assertTrue(t1.compareTo(t3) < 0);
        assertTrue(t1.compareTo(t4) < 0);

        assertTrue(t2.compareTo(t0) > 0);
        assertTrue(t2.compareTo(t1) > 0);
        assertTrue(t2.compareTo(t3) < 0);
        assertTrue(t2.compareTo(t4) < 0);

        assertTrue(t3.compareTo(t0) > 0);
        assertTrue(t3.compareTo(t1) > 0);
        assertTrue(t3.compareTo(t2) > 0);
        assertTrue(t3.compareTo(t4) < 0);

        assertTrue(t4.compareTo(t0) > 0);
        assertTrue(t4.compareTo(t1) > 0);
        assertTrue(t4.compareTo(t2) > 0);
        assertTrue(t4.compareTo(t3) > 0);
    }

    @Test
    public void compare_on_value() {
        final Tags t0 = new Tags(singletonMap("a", MetricValue.fromIntValue(7)));
        final Tags t1 = new Tags(singletonMap("a", MetricValue.fromIntValue(9)));
        final Tags t2 = new Tags(singletonMap("a", MetricValue.TRUE));

        assertTrue(t0.compareTo(t1) < 0);
        assertTrue(t0.compareTo(t2) < 0);

        assertTrue(t1.compareTo(t0) > 0);
        assertTrue(t1.compareTo(t2) < 0);

        assertTrue(t2.compareTo(t0) > 0);
        assertTrue(t2.compareTo(t1) > 0);
    }
}
