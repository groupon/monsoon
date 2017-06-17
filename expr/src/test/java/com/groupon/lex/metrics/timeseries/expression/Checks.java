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
package com.groupon.lex.metrics.timeseries.expression;

import com.groupon.lex.metrics.timeseries.TimeSeriesMetricDeltaSet;
import java.util.stream.Stream;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author ariane
 */
public class Checks {
    private Checks() {}

    public static void testResultEmpty(TimeSeriesMetricDeltaSet result) {
        assertTrue(result.isEmpty() || result.isScalar());
        result.streamValues()
                .forEach((ts_delta) -> {
                    assertFalse(ts_delta.isPresent());
                });
    }

    public static void testResult(long expected, TimeSeriesMetricDeltaSet result) {
        assertFalse(result.isEmpty());
        result.streamValues()
                .map((ts_delta) -> {
                    assertTrue(ts_delta.isPresent());
                    assertTrue(ts_delta.value().isPresent());
                    return ts_delta.value();
                })
                .flatMap((opt) -> opt.map(Stream::of).orElseGet(Stream::empty))
                .forEach((Number number) -> {
                    assertEquals(expected, number);
                });
    }

    public static void testResult(double expected, TimeSeriesMetricDeltaSet result) {
        assertFalse(result.isEmpty());
        result.streamValues()
                .map((ts_delta) -> {
                    assertTrue(ts_delta.isPresent());
                    assertTrue(ts_delta.value().isPresent());
                    return ts_delta.value();
                })
                .flatMap((opt) -> opt.map(Stream::of).orElseGet(Stream::empty))
                .forEach((Number number) -> {
                    assertEquals(expected, number);
                });
    }

    public static void testResult(boolean expected, TimeSeriesMetricDeltaSet result) {
        assertFalse(result.isEmpty());
        result.streamValues()
                .map((ts_delta) -> {
                    assertTrue(ts_delta.isPresent());
                    assertNotNull(ts_delta.getBoolValue());
                    return ts_delta.getBoolValue();
                })
                .forEach((Boolean bool) -> {
                    assertEquals(expected, bool);
                });
    }

    public static void testResult(String expected, TimeSeriesMetricDeltaSet result) {
        assertFalse(result.isEmpty());
        result.streamValues()
                .map((ts_delta) -> {
                    assertTrue(ts_delta.isPresent());
                    assertNotNull(ts_delta.getStrValue());
                    return ts_delta.getStrValue();
                })
                .forEach((String string) -> {
                    assertEquals(expected, string);
                });
    }

    public static void testCoercion(long expected, TimeSeriesMetricDeltaSet result) {
        assertFalse(result.isEmpty());
        result.streamValues()
                .map((ts_delta) -> {
                    assertTrue(ts_delta.isPresent());
                    assertNull("Doesn't hold an actual integral.", ts_delta.getIntValue());
                    assertTrue("Is coercible to integral.", ts_delta.value().isPresent());
                    return ts_delta.value();
                })
                .flatMap((opt) -> opt.map(Stream::of).orElseGet(Stream::empty))
                .forEach((Number number) -> {
                    assertEquals(expected, number);
                });
    }

    public static void testCoercion(double expected, TimeSeriesMetricDeltaSet result) {
        assertFalse(result.isEmpty());
        result.streamValues()
                .map((ts_delta) -> {
                    assertTrue(ts_delta.isPresent());
                    assertNull("Doesn't hold an actual floating point.", ts_delta.getFltValue());
                    assertTrue("Is coercible to floating point.", ts_delta.value().isPresent());
                    return ts_delta.value();
                })
                .flatMap((opt) -> opt.map(Stream::of).orElseGet(Stream::empty))
                .forEach((Number number) -> {
                    assertEquals(expected, number);
                });
    }

    public static void testCoercion(boolean expected, TimeSeriesMetricDeltaSet result) {
        assertFalse(result.isEmpty());
        result.streamValues()
                .map((ts_delta) -> {
                    assertTrue(ts_delta.isPresent());
                    assertNull("Doesn't hold an actual boolean.", ts_delta.getBoolValue());
                    assertTrue("Is coercible to boolean.", ts_delta.asBool().isPresent());
                    return ts_delta.asBool();
                })
                .flatMap((opt) -> opt.map(Stream::of).orElseGet(Stream::empty))
                .forEach((Boolean bool) -> {
                    assertEquals(expected, bool);
                });
    }
}
