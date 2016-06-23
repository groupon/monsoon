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

import java.util.Optional;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class Any2Test {
    @Test
    public void left_constructor() {
        Any2<String, String> any2 = Any2.left("foo");

        assertEquals(Optional.of("foo"), any2.getLeft());
        assertEquals(Optional.empty(), any2.getRight());
        assertThat(any2.toString(), containsString("foo"));
    }

    @Test
    public void right_constructor() {
        Any2<String, String> any2 = Any2.right("foo");

        assertEquals(Optional.of("foo"), any2.getRight());
        assertEquals(Optional.empty(), any2.getLeft());
        assertThat(any2.toString(), containsString("foo"));
    }

    @Test
    public void map() {
        Any2<String, String> left = Any2.left("foo");
        Any2<String, String> right = Any2.right("fizzbuzz");

        assertEquals(Any2.left(3), left.map(String::length, (s) -> { fail(); return 0; }));
        assertEquals(Any2.right(8), right.map((s) -> { fail(); return 0; }, String::length));
    }

    @Test
    public void map_combine() {
        Any2<String, String> left = Any2.left("foo");
        Any2<String, String> right = Any2.right("fizzbuzz");

        assertEquals(Integer.valueOf(3), left.mapCombine(String::length, (s) -> { fail(); return 0; }));
        assertEquals(Integer.valueOf(8), right.mapCombine((s) -> { fail(); return 0; }, String::length));
    }
}
