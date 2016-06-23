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
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class SimpleGroupPathTest {
    @Test
    public void empty() {
        SimpleGroupPath p = new SimpleGroupPath();

        assertTrue(p.getPath().isEmpty());
        assertEquals("", p.configString().toString());
        assertEquals("", p.toString());
    }

    @Test
    public void not_empty() {
        SimpleGroupPath p = new SimpleGroupPath("foo");

        assertFalse(p.getPath().isEmpty());
        assertThat(p.getPath(), hasItem("foo"));
        assertEquals("foo", p.configString().toString());
        assertEquals("foo", p.toString());
    }

    @Test
    public void long_name_and_escapes() {
        SimpleGroupPath p = new SimpleGroupPath("\r", "", "\b");

        assertEquals("'\\r'.''.'\\b'", p.configString().toString());
    }

    @Test
    public void equality() {
        SimpleGroupPath p0 = new SimpleGroupPath("foo", "bar");
        SimpleGroupPath p1 = new SimpleGroupPath(Arrays.asList("foo", "bar"));

        assertNotSame(p0, p1);
        assertEquals(p0, p1);
        assertEquals(p0.hashCode(), p1.hashCode());
        assertEquals(0, p0.compareTo(p1));
    }

    @Test
    public void inequality() {
        SimpleGroupPath p0 = new SimpleGroupPath();
        SimpleGroupPath p1 = new SimpleGroupPath("a");
        SimpleGroupPath p2 = new SimpleGroupPath("a", "b");
        SimpleGroupPath p3 = new SimpleGroupPath("b");

        assertNotEquals(p0, p1);
        assertNotEquals(p0, p2);
        assertNotEquals(p0, p3);

        assertNotEquals(p1, p0);
        assertNotEquals(p1, p2);
        assertNotEquals(p1, p3);

        assertNotEquals(p2, p0);
        assertNotEquals(p2, p1);
        assertNotEquals(p2, p3);

        assertNotEquals(p3, p0);
        assertNotEquals(p3, p1);
        assertNotEquals(p3, p2);
    }

    @Test
    public void compare() {
        SimpleGroupPath p0 = new SimpleGroupPath();
        SimpleGroupPath p1 = new SimpleGroupPath("a");
        SimpleGroupPath p2 = new SimpleGroupPath("a", "b");
        SimpleGroupPath p3 = new SimpleGroupPath("b");

        assertTrue(p0.compareTo(p1) < 0);
        assertTrue(p0.compareTo(p2) < 0);
        assertTrue(p0.compareTo(p3) < 0);

        assertTrue(p1.compareTo(p0) > 0);
        assertTrue(p1.compareTo(p2) < 0);
        assertTrue(p1.compareTo(p3) < 0);

        assertTrue(p2.compareTo(p0) > 0);
        assertTrue(p2.compareTo(p1) > 0);
        assertTrue(p2.compareTo(p3) < 0);

        assertTrue(p3.compareTo(p0) > 0);
        assertTrue(p3.compareTo(p1) > 0);
        assertTrue(p3.compareTo(p2) > 0);

        assertTrue(p0.compareTo(null) > 0);
        assertTrue(p1.compareTo(null) > 0);
        assertTrue(p2.compareTo(null) > 0);
        assertTrue(p3.compareTo(null) > 0);
    }

    @Test
    public void equal_across_types() {
        assertFalse(new SimpleGroupPath().equals(null));
        assertFalse(new SimpleGroupPath().equals(new Object()));
    }
}
