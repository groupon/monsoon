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
package com.groupon.lex.metrics.lib;

import java.util.function.Function;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class MemoidOneTest {
    @Test
    public void test_invocation() {
        final MemoidOne<Integer, String> memoid = new MemoidOne<>((i) -> i.toString());

        assertEquals("7", memoid.apply(7));
        assertEquals("19", memoid.apply(19));
        assertEquals("19", memoid.apply(19));
        assertEquals("7", memoid.apply(7));
    }

    @Test
    public void test_invocation_remembers() {
        final MemoidOne<Integer, String> memoid = new MemoidOne<>(new Function<Integer, String>() {
            private int invocation_count = 0;

            @Override
            public String apply(Integer i) {
                assertEquals("Memoid must skip subsequent invocations, it has been invoked before", 0, invocation_count);
                ++invocation_count;
                return i.toString();
            }
        });

        assertEquals("first invocation (will trigger invocation of wrapped function)", "19", memoid.apply(19));
        assertEquals("second invocation (won't trigger invocation)", "19", memoid.apply(19));
        assertEquals("third invocation (won't trigger invocation)", "19", memoid.apply(19));
    }

    @Test
    public void always_call_for_null() {
        final MemoidOne<Integer, Integer> memoid = new MemoidOne<>(new Function<Integer, Integer>() {
            private int invocation_count = 0;

            @Override
            public Integer apply(Integer i) {
                return ++invocation_count;
            }
        });

        assertEquals(Integer.valueOf(1), memoid.apply(null));
        assertEquals(Integer.valueOf(2), memoid.apply(null));
        assertEquals(Integer.valueOf(3), memoid.apply(null));
        assertEquals(Integer.valueOf(4), memoid.apply(null));
    }
}
