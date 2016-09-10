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
package com.groupon.lex.metrics.resolver;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class RangeResolverTest {
    @Test
    public void values() {
        RangeResolver range = new RangeResolver(-1, 1);

        assertEquals(1, range.getTupleWidth());
        assertEquals(-1, range.getBegin());
        assertEquals(1, range.getEnd());
        assertThat(range.getTuples(),
                containsInAnyOrder(
                        new ResolverTuple(ResolverTuple.newTupleElement(-1)),
                        new ResolverTuple(ResolverTuple.newTupleElement(0)),
                        new ResolverTuple(ResolverTuple.newTupleElement(1))));
        assertEquals("-1..1", range.configString());
    }

    @Test
    public void negativeRange() {
        RangeResolver range = new RangeResolver(1, -1);

        assertEquals(1, range.getTupleWidth());
        assertEquals(1, range.getBegin());
        assertEquals(-1, range.getEnd());
        assertTrue(range.getTuples().isEmpty());
        assertEquals("1..-1", range.configString());
    }

    @Test
    public void singleElementRange() {
        RangeResolver range = new RangeResolver(17, 17);

        assertEquals(1, range.getTupleWidth());
        assertEquals(17, range.getBegin());
        assertEquals(17, range.getEnd());
        assertThat(range.getTuples(),
                containsInAnyOrder(
                        new ResolverTuple(ResolverTuple.newTupleElement(17))));
        assertEquals("17..17", range.configString());
    }

    @Test
    public void intMinValue() {
        RangeResolver range = new RangeResolver(Integer.MIN_VALUE, Integer.MIN_VALUE);

        assertEquals(1, range.getTupleWidth());
        assertEquals(Integer.MIN_VALUE, range.getBegin());
        assertEquals(Integer.MIN_VALUE, range.getEnd());
        assertThat(range.getTuples(),
                containsInAnyOrder(
                        new ResolverTuple(ResolverTuple.newTupleElement(Integer.MIN_VALUE))));
        assertEquals(Integer.MIN_VALUE + ".." + Integer.MIN_VALUE, range.configString());
    }

    @Test
    public void intMaxValue() {
        RangeResolver range = new RangeResolver(Integer.MAX_VALUE, Integer.MAX_VALUE);

        assertEquals(1, range.getTupleWidth());
        assertEquals(Integer.MAX_VALUE, range.getBegin());
        assertEquals(Integer.MAX_VALUE, range.getEnd());
        assertThat(range.getTuples(),
                containsInAnyOrder(
                        new ResolverTuple(ResolverTuple.newTupleElement(Integer.MAX_VALUE))));
        assertEquals(Integer.MAX_VALUE + ".." + Integer.MAX_VALUE, range.configString());
    }
}
