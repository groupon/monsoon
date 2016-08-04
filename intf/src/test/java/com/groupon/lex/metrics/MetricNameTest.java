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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class MetricNameTest {
    @Test
    public void no_arg_constructor() {
        final MetricName name = MetricName.valueOf();

        assertTrue(name.getPath().isEmpty());
        assertEquals("", name.getPathString());
        assertEquals("", name.configString().toString());
        assertEquals(MetricName.valueOf(), name);
        assertEquals(0, name.compareTo(MetricName.valueOf()));
    }

    @Test
    public void empty_list_constructor() {
        final MetricName name = MetricName.valueOf(EMPTY_LIST);

        assertTrue(name.getPath().isEmpty());
        assertEquals("", name.getPathString());
        assertEquals("", name.configString().toString());
        assertEquals(MetricName.valueOf(), name);
        assertEquals(0, name.compareTo(MetricName.valueOf()));
    }

    @Test
    public void equality() {
        final MetricName n0 = MetricName.valueOf("foo", "bar");
        final MetricName n1 = MetricName.valueOf(Arrays.asList("foo", "bar"));

        assertEquals(n0, n1);
        assertEquals(0, n0.compareTo(n1));
    }

    @Test
    public void inequality() {
        final MetricName n0 = MetricName.valueOf("a", "a");
        final MetricName n1 = MetricName.valueOf("a", "b");

        assertNotEquals(n0, n1);
        assertTrue(n0.compareTo(n1) < 0);
        assertTrue(n1.compareTo(n0) > 0);
    }

    @Test
    public void getPathString() {
        assertEquals("foo", MetricName.valueOf("foo").getPathString());
        assertEquals("'99'", MetricName.valueOf("99").getPathString());
        assertEquals("'\\r'", MetricName.valueOf("\r").getPathString());
        assertEquals("'3'.'14159'", MetricName.valueOf("3", "14159").getPathString());
        assertEquals("''", MetricName.valueOf("").getPathString());
    }

    @Test
    public void configString() {
        assertEquals("foo", MetricName.valueOf("foo").configString().toString());
        assertEquals("'99'", MetricName.valueOf("99").configString().toString());
        assertEquals("'\\r'", MetricName.valueOf("\r").configString().toString());
        assertEquals("'3'.'14159'", MetricName.valueOf("3", "14159").configString().toString());
        assertEquals("''", MetricName.valueOf("").configString().toString());
    }

    @Test
    public void to_string() {
        assertEquals("foo", MetricName.valueOf("foo").toString());
        assertEquals("'99'", MetricName.valueOf("99").toString());
        assertEquals("'\\r'", MetricName.valueOf("\r").toString());
        assertEquals("'3'.'14159'", MetricName.valueOf("3", "14159").toString());
        assertEquals("''", MetricName.valueOf("").toString());
    }

    @Test
    public void equals_across_type() {
        assertFalse(MetricName.valueOf().equals(null));
        assertFalse(MetricName.valueOf().equals(new Object()));
    }

    @Test
    public void compare() {
        final MetricName t0 = MetricName.valueOf("a");
        final MetricName t1 = MetricName.valueOf("a", "a");
        final MetricName t2 = MetricName.valueOf("a", "b");

        assertTrue(t0.compareTo(t1) < 0);
        assertTrue(t0.compareTo(t2) < 0);

        assertTrue(t1.compareTo(t0) > 0);
        assertTrue(t1.compareTo(t2) < 0);

        assertTrue(t2.compareTo(t0) > 0);
        assertTrue(t2.compareTo(t1) > 0);

        assertTrue(t0.compareTo(null) > 0);
        assertTrue(t1.compareTo(null) > 0);
        assertTrue(t2.compareTo(null) > 0);
    }
}
