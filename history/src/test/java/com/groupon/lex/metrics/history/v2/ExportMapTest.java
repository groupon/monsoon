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
package com.groupon.lex.metrics.history.v2;

import static java.util.Collections.unmodifiableMap;
import java.util.HashMap;
import java.util.Map;
import org.hamcrest.Matchers;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class ExportMapTest {
    private static final Map<Integer, String> INIT_MAP = unmodifiableMap(new HashMap<Integer, String>() {{
        put(0, "zero");
        put(1, "one");
        put(2, "two");
        put(3, "three");
    }});

    @Test
    public void defaultConstructor() {
        ExportMap<String> map = new ExportMap<>();

        assertTrue(map.isEmpty());
        assertEquals(0, map.getOffset());
        assertThat(map.invert(), Matchers.empty());
    }

    @Test
    public void mapConstructorStartingAfter() {
        ExportMap<String> map = new ExportMap<>(INIT_MAP.size(), INIT_MAP);

        assertTrue(map.isEmpty());
        assertEquals(INIT_MAP.size(), map.getOffset());
        assertThat(map.createMap(), Matchers.empty());
        assertThat(map.invert(), Matchers.contains("zero", "one", "two", "three"));

        assertEquals(1, map.getOrCreate("one"));  // Request existing item.
        assertEquals(INIT_MAP.size(), map.getOrCreate("not yet in this map"));  // Add new item.
        assertThat(map.createMap(), Matchers.contains("not yet in this map"));
        assertThat(map.invert(), Matchers.contains("zero", "one", "two", "three", "not yet in this map"));
    }

    @Test
    public void mapConstructorStartingBefore() {
        ExportMap<String> map = new ExportMap<>(1, INIT_MAP);

        assertFalse(map.isEmpty());
        assertEquals(1, map.getOffset());
        assertThat(map.createMap(), Matchers.contains("one", "two", "three"));
        assertThat(map.invert(), Matchers.contains("zero", "one", "two", "three"));

        assertEquals(1, map.getOrCreate("one"));
        assertEquals(INIT_MAP.size(), map.getOrCreate("not yet in this map"));
        assertThat(map.createMap(), Matchers.contains("one", "two", "three", "not yet in this map"));
        assertThat(map.invert(), Matchers.contains("zero", "one", "two", "three", "not yet in this map"));
    }

    @Test
    public void reset() {
        ExportMap<String> map = new ExportMap<>(1, INIT_MAP);
        map.reset();

        assertTrue(map.isEmpty());
        assertEquals(new ExportMap<>(INIT_MAP.size(), INIT_MAP), map);
    }

    @Test
    public void cloneMap() {
        ExportMap<String> original = new ExportMap<>(1, INIT_MAP);
        ExportMap<String> copy = original.clone();

        assertNotSame(original, copy);
        assertEquals(original, copy);
    }
}
