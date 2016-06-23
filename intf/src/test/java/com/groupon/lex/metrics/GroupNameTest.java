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

import java.util.Arrays;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class GroupNameTest {
    private static final SimpleGroupPath PATH = new SimpleGroupPath("com", "groupon", "lex", "jmx_monitord", "Awesomium");

    @Test
    public void constructor() {
        GroupName group_name = new GroupName(PATH);

        assertEquals(PATH, group_name.getPath());
        assertTrue(group_name.getTags().isEmpty());
    }

    @Test
    public void constructor_direct() {
        GroupName foo_bar = new GroupName("foo", "bar");
        GroupName fizzbuzz = new GroupName("fizzbuzz");

        assertEquals(Arrays.asList("foo", "bar"), foo_bar.getPath().getPath());
        assertEquals(Arrays.asList("fizzbuzz"), fizzbuzz.getPath().getPath());
        assertTrue(foo_bar.getTags().isEmpty());
        assertTrue(fizzbuzz.getTags().isEmpty());
    }

    @Test
    public void equality_with_shared_initializer() {
        GroupName g1 = new GroupName(PATH);
        GroupName g2 = new GroupName(PATH);

        assertEquals(g1, g2);
    }

    @Test
    public void inequality() {
        GroupName g1 = new GroupName(new SimpleGroupPath("foo", "bar"));
        GroupName g2 = new GroupName(new SimpleGroupPath("foo", "baz"));
        GroupName g3 = new GroupName(new SimpleGroupPath("foo", "baz"), new Tags(singletonMap("foo", MetricValue.TRUE)));
        GroupName g4 = new GroupName(new SimpleGroupPath("foo", "baz"), new Tags(singletonMap("foo", MetricValue.FALSE)));

        assertNotEquals(g1, g2);
        assertNotEquals(g1, g3);
        assertNotEquals(g1, g4);

        assertNotEquals(g2, g1);
        assertNotEquals(g2, g3);
        assertNotEquals(g2, g4);

        assertNotEquals(g3, g1);
        assertNotEquals(g3, g2);
        assertNotEquals(g3, g4);

        assertNotEquals(g4, g1);
        assertNotEquals(g4, g2);
        assertNotEquals(g4, g3);
    }

    @Test
    public void string() {
        GroupName group_name = new GroupName(PATH);

        assertEquals(PATH.toString(), group_name.toString());
    }

    @Test
    public void string_quotes() {
        GroupName group_name = new GroupName(new SimpleGroupPath("99", "Luftballons"));

        assertEquals("'99'.Luftballons", group_name.configString().toString());
    }

    @Test
    public void streamed_tags_construction() {
        GroupName group_name = new GroupName(new SimpleGroupPath("foo", "bar"), singletonMap("key", MetricValue.TRUE).entrySet().stream());

        assertEquals("foo.bar{key=true}", group_name.configString().toString());
    }

    @Test
    public void equals_across_types() {
        assertFalse(new GroupName("foo").equals(null));
        assertFalse(new GroupName("foo").equals(new Object()));
    }

    @Test
    public void hash_code_equality() {
        GroupName g1 = new GroupName(new SimpleGroupPath("foo"), new Tags(singletonMap("bar", MetricValue.fromStrValue("baz"))));
        GroupName g2 = new GroupName(new SimpleGroupPath("foo"), new Tags(singletonMap("bar", MetricValue.fromStrValue("baz"))));

        assertNotSame(g1, g2);
        assertEquals(g1, g2);
        assertEquals(g1.hashCode(), g2.hashCode());
    }

    @Test
    public void inequality_comparison() {
        GroupName g0 = new GroupName(new SimpleGroupPath(EMPTY_LIST));
        GroupName g1 = new GroupName(new SimpleGroupPath("foo", "bar"));
        GroupName g2 = new GroupName(new SimpleGroupPath("foo", "baz"));
        GroupName g3 = new GroupName(new SimpleGroupPath("foo", "baz"), new Tags(singletonMap("foo", MetricValue.FALSE)));
        GroupName g4 = new GroupName(new SimpleGroupPath("foo", "baz"), new Tags(singletonMap("foo", MetricValue.TRUE)));

        assertTrue(g0.compareTo(null) > 0);
        assertTrue(g0.compareTo(g1) < 0);
        assertTrue(g0.compareTo(g2) < 0);
        assertTrue(g0.compareTo(g3) < 0);
        assertTrue(g0.compareTo(g4) < 0);

        assertTrue(g1.compareTo(null) > 0);
        assertTrue(g1.compareTo(g0) > 0);
        assertTrue(g1.compareTo(g2) < 0);
        assertTrue(g1.compareTo(g3) < 0);
        assertTrue(g1.compareTo(g4) < 0);

        assertTrue(g2.compareTo(null) > 0);
        assertTrue(g2.compareTo(g0) > 0);
        assertTrue(g2.compareTo(g1) > 0);
        assertTrue(g2.compareTo(g3) < 0);
        assertTrue(g2.compareTo(g4) < 0);

        assertTrue(g3.compareTo(null) > 0);
        assertTrue(g3.compareTo(g0) > 0);
        assertTrue(g3.compareTo(g1) > 0);
        assertTrue(g3.compareTo(g2) > 0);
        assertTrue(g3.compareTo(g4) < 0);

        assertTrue(g4.compareTo(null) > 0);
        assertTrue(g4.compareTo(g0) > 0);
        assertTrue(g4.compareTo(g1) > 0);
        assertTrue(g4.compareTo(g2) > 0);
        assertTrue(g4.compareTo(g3) > 0);
    }
}
