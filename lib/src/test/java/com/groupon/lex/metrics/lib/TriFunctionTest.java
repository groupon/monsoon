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

import java.util.function.BiFunction;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class TriFunctionTest {
    private static final TriFunction<Integer, Integer, Integer, Integer> fn = (x, y, z) -> x + y + z;

    @Test
    public void apply() {
        assertEquals(Integer.valueOf(6), fn.apply(1, 2, 3));
    }

    @Test
    public void and_then() {
        assertEquals(Integer.valueOf(12), fn.andThen(x -> 2 * x).apply(1, 2, 3));
    }

    @Test
    public void bind_value() {
        BiFunction<Integer, Integer, Integer> first = fn.bind1(1);
        BiFunction<Integer, Integer, Integer> second = fn.bind2(2);
        BiFunction<Integer, Integer, Integer> third = fn.bind3(3);

        assertEquals(Integer.valueOf(17), first.apply(7, 9));
        assertEquals(Integer.valueOf(18), second.apply(7, 9));
        assertEquals(Integer.valueOf(19), third.apply(7, 9));
    }

    @Test
    public void bind_transformation() {
        TriFunction<Integer, Integer, Integer, Integer> first = fn.bind1_fn(x -> -x);
        TriFunction<Integer, Integer, Integer, Integer> second = fn.bind2_fn(x -> -x);
        TriFunction<Integer, Integer, Integer, Integer> third = fn.bind3_fn(x -> -x);

        assertEquals(Integer.valueOf(-1 + 2 + 3), first.apply(1, 2, 3));
        assertEquals(Integer.valueOf(1 + -2 + 3), second.apply(1, 2, 3));
        assertEquals(Integer.valueOf(1 + 2 + -3), third.apply(1, 2, 3));
    }
}
