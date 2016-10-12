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

import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.history.v2.tables.DictionaryDelta;
import java.util.Arrays;
import static java.util.Collections.singletonMap;
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
public class DictionaryForWriteTest {
    @Test
    public void defaultConstructor() {
        DictionaryForWrite dfw = new DictionaryForWrite();

        assertTrue(dfw.isEmpty());
        assertEquals(0, dfw.getStringTable().getOffset());
        assertEquals(0, dfw.getPathTable().getOffset());
        assertEquals(0, dfw.getTagsTable().getOffset());

        assertEquals(0, dfw.encode().sdd.offset);
        assertThat(dfw.encode().sdd.values, Matchers.emptyArray());
        assertEquals(0, dfw.encode().pdd.offset);
        assertThat(dfw.encode().pdd.values, Matchers.emptyArray());
        assertEquals(0, dfw.encode().tdd.offset);
        assertThat(dfw.encode().tdd.values, Matchers.emptyArray());

        assertEquals(0, dfw.asDictionaryDelta().getStringRefOffset());
        assertEquals(0, dfw.asDictionaryDelta().getStringRefEnd());
        assertEquals(0, dfw.asDictionaryDelta().getPathRefOffset());
        assertEquals(0, dfw.asDictionaryDelta().getPathRefEnd());
        assertEquals(0, dfw.asDictionaryDelta().getTagsRefOffset());
        assertEquals(0, dfw.asDictionaryDelta().getTagsRefEnd());
    }

    @Test
    public void reset() {
        DictionaryForWrite dfw = new DictionaryForWrite();
        int lastStr = dfw.getStringTable().getOrCreate("foobar");
        int lastPath = dfw.getPathTable().getOrCreate(Arrays.asList("foo", "bar"));
        int lastTags = dfw.getTagsTable().getOrCreate(Tags.valueOf(singletonMap("baz", MetricValue.fromStrValue("baz"))));

        assertFalse(dfw.isEmpty());
        assertEquals(0, dfw.getStringTable().getOffset());
        assertEquals(0, dfw.getPathTable().getOffset());
        assertEquals(0, dfw.getTagsTable().getOffset());

        dfw.reset();

        assertTrue(dfw.isEmpty());
        assertEquals(lastStr + 1,  dfw.getStringTable().getOffset());
        assertEquals(lastPath + 1, dfw.getPathTable().getOffset());
        assertEquals(lastTags + 1, dfw.getTagsTable().getOffset());
    }

    @Test
    public void cloneDFW() {
        // Setup original with some data.
        DictionaryForWrite original = new DictionaryForWrite();
        original.getStringTable().getOrCreate("foobar");
        original.getPathTable().getOrCreate(Arrays.asList("foo", "bar"));
        original.getTagsTable().getOrCreate(Tags.valueOf(singletonMap("baz", MetricValue.fromStrValue("baz"))));

        DictionaryForWrite copy = original.clone();

        assertNotSame(original, copy);
        assertNotSame(original.getStringTable(), copy.getStringTable());
        assertNotSame(original.getPathTable(), copy.getPathTable());
        assertNotSame(original.getTagsTable(), copy.getTagsTable());

        assertEquals(original, copy);
    }

    @Test
    public void encode() {
        DictionaryForWrite dfw = new DictionaryForWrite();
        int lastStr = dfw.getStringTable().getOrCreate("foobar");
        int lastPath = dfw.getPathTable().getOrCreate(Arrays.asList("foo", "bar"));
        int lastTags = dfw.getTagsTable().getOrCreate(Tags.valueOf(singletonMap("baz", MetricValue.fromStrValue("baz"))));

        DictionaryDelta dd = new DictionaryDelta(dfw.encode());  // Encoded form, read back.
        assertEquals(dfw.asDictionaryDelta(), dd);

        dfw.reset();  // From constructor acts as load + reset.
        assertEquals(dfw, new DictionaryForWrite(dd));
    }
}
