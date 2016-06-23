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

import static com.groupon.lex.metrics.AttributeConverter.resolve_property;
import java.util.ArrayList;
import java.util.Arrays;
import static java.util.Collections.singletonMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class AttributeConverterTest {
    private static final List<String> ROOT = Arrays.asList("root");

    private static MetricName extend_(String... elems) {
        ArrayList<String> list = new ArrayList<>(ROOT);
        Arrays.stream(elems).forEachOrdered(list::add);
        return new MetricName(list);
    }

    @Test
    public void null_attr() {
        final Map<MetricName, MetricValue> result = resolve_property(ROOT, null)
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

        assertTrue(result.isEmpty());
    }

    @Test
    public void bool_attr() {
        final Map<MetricName, MetricValue> result = resolve_property(ROOT, Boolean.TRUE)
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

        assertThat(result, hasEntry(new MetricName(ROOT), MetricValue.TRUE));
    }

    @Test
    public void byte_attr() {
        final Map<MetricName, MetricValue> result = resolve_property(ROOT, Byte.valueOf((byte)7))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

        assertThat(result, hasEntry(new MetricName(ROOT), MetricValue.fromIntValue(7)));
    }

    @Test
    public void short_attr() {
        final Map<MetricName, MetricValue> result = resolve_property(ROOT, Short.valueOf((short)7))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

        assertThat(result, hasEntry(new MetricName(ROOT), MetricValue.fromIntValue(7)));
    }

    @Test
    public void int_attr() {
        final Map<MetricName, MetricValue> result = resolve_property(ROOT, Integer.valueOf(7))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

        assertThat(result, hasEntry(new MetricName(ROOT), MetricValue.fromIntValue(7)));
    }

    @Test
    public void long_attr() {
        final Map<MetricName, MetricValue> result = resolve_property(ROOT, Long.valueOf(7))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

        assertThat(result, hasEntry(new MetricName(ROOT), MetricValue.fromIntValue(7)));
    }

    @Test
    public void float_attr() {
        final Map<MetricName, MetricValue> result = resolve_property(ROOT, Float.valueOf(7))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

        assertThat(result, hasEntry(new MetricName(ROOT), MetricValue.fromDblValue(7)));
    }

    @Test
    public void double_attr() {
        final Map<MetricName, MetricValue> result = resolve_property(ROOT, Double.valueOf(7))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

        assertThat(result, hasEntry(new MetricName(ROOT), MetricValue.fromDblValue(7)));
    }

    @Test
    public void string_attr() {
        final Map<MetricName, MetricValue> result = resolve_property(ROOT, "foobar")
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

        assertThat(result, hasEntry(new MetricName(ROOT), MetricValue.fromStrValue("foobar")));
    }

    @Test
    public void list_attr() {
        final Map<MetricName, MetricValue> result = resolve_property(ROOT, Arrays.asList("foo", null, "bar"))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

        assertThat(result, allOf(
                hasEntry(extend_("0"), MetricValue.fromStrValue("foo")),
                hasEntry(extend_("2"), MetricValue.fromStrValue("bar"))));
        assertThat(result, not(
                hasEntry(extend_("1"), MetricValue.EMPTY)));
    }

    @Test
    public void set_attr() {
        final Map<MetricName, MetricValue> result = resolve_property(ROOT, new HashSet<String>(Arrays.asList("foo", "bar")))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

        assertTrue(result.isEmpty());
    }

    @Test
    public void map_attr() {
        final Map<MetricName, MetricValue> result = resolve_property(ROOT, new HashMap() {{
                    put("7", "foo");
                    put("false", true);
                    put("nil", null);
                }})
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

        assertThat(result, allOf(
                hasEntry(extend_("7"), MetricValue.fromStrValue("foo")),
                hasEntry(extend_("false"), MetricValue.TRUE)));
        assertThat(result, not(
                hasEntry(extend_("nil"), MetricValue.EMPTY)));
    }

    @Test
    public void list_recursive_attr() {
        final Map<MetricName, MetricValue> result = resolve_property(ROOT, Arrays.asList(
                    singletonMap("foo", "bar"),
                    Arrays.asList("baz")
                ))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

        assertThat(result, allOf(
                hasEntry(extend_("0", "foo"), MetricValue.fromStrValue("bar")),
                hasEntry(extend_("1", "0"), MetricValue.fromStrValue("baz"))));
        assertThat(result, allOf(
                not(hasKey(extend_("0"))),
                not(hasKey(extend_("1")))));
    }

    @Test
    public void map_recursive_attr() {
        final Map<MetricName, MetricValue> result = resolve_property(ROOT, new HashMap() {{
                    put("map", singletonMap("foo", "bar"));
                    put("list", Arrays.asList("baz"));
                }})
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

        assertThat(result, allOf(
                hasEntry(extend_("map", "foo"), MetricValue.fromStrValue("bar")),
                hasEntry(extend_("list", "0"), MetricValue.fromStrValue("baz"))));
        assertThat(result, allOf(
                not(hasKey(extend_("map"))),
                not(hasKey(extend_("list")))));
    }

    @Test
    public void unrecognized_attr() {
        final Map<MetricName, MetricValue> result = resolve_property(ROOT, new Object())
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

        assertTrue(result.isEmpty());
    }
}
