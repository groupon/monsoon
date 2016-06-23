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

import java.util.Optional;
import java.util.stream.Stream;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class MetricValueTest {
    public static final byte SEVEN_BYTE = 7;
    public static final short SEVEN_SHORT = 7;
    public static final int SEVEN_INT = 7;
    public static final long SEVEN_LONG = 7;
    public static final float SEVEN_FLOAT = 7;
    public static final double SEVEN_DOUBLE = 7;
    public static final Histogram hist42 = new Histogram(Stream.of(new Histogram.RangeWithCount(new Histogram.Range(0, 10), 42)));
    public static final Histogram hist24 = new Histogram(Stream.of(new Histogram.RangeWithCount(new Histogram.Range(0, 10), 24)));

    public static void validateNumber(boolean expected_bool, long expected_number, MetricValue mv) {
        assertNull(mv.getBoolValue());
        assertEquals(Long.valueOf(expected_number), mv.getIntValue());
        assertNull(mv.getFltValue());
        assertNull(mv.getStrValue());
        assertNull(mv.getHistValue());
        assertFalse(mv.histogram().isPresent());

        assertTrue(mv.value().isPresent());
        assertEquals(Long.valueOf(expected_number), mv.value().get());

        assertTrue(mv.asBool().isPresent());
        assertEquals(expected_bool, mv.asBool().get());

        assertFalse(mv.stringValue().isPresent());
    }

    public static void validateNumber(boolean expected_bool, double expected_number, MetricValue mv) {
        assertNull(mv.getBoolValue());
        assertNull(mv.getIntValue());
        assertEquals(Double.valueOf(expected_number), mv.getFltValue());
        assertNull(mv.getStrValue());
        assertNull(mv.getHistValue());
        assertFalse(mv.histogram().isPresent());

        assertTrue(mv.value().isPresent());
        assertEquals(Double.valueOf(expected_number), mv.value().get());

        assertTrue(mv.asBool().isPresent());
        assertEquals(expected_bool, mv.asBool().get());

        assertFalse(mv.stringValue().isPresent());
    }

    public static void validateBoolean(boolean expected_bool, MetricValue mv) {
        assertNotNull(mv.getBoolValue());
        assertNull(mv.getIntValue());
        assertNull(mv.getFltValue());
        assertNull(mv.getStrValue());
        assertNull(mv.getHistValue());
        assertTrue(mv.asBool().isPresent());
        assertTrue(mv.value().isPresent());
        assertFalse(mv.stringValue().isPresent());
        assertFalse(mv.histogram().isPresent());

        assertEquals(Boolean.valueOf(expected_bool), mv.getBoolValue());
        assertEquals(Boolean.valueOf(expected_bool), mv.asBool().get());
        assertEquals(expected_bool ? Long.valueOf(1) : Long.valueOf(0), mv.value().get());
    }

    public static void validateString(String expected, MetricValue mv) {
        assertNull(mv.getBoolValue());
        assertNull(mv.getIntValue());
        assertNull(mv.getFltValue());
        assertNotNull(mv.getStrValue());
        assertNull(mv.getHistValue());
        assertFalse(mv.asBool().isPresent());
        assertFalse(mv.value().isPresent());
        assertTrue(mv.stringValue().isPresent());
        assertFalse(mv.histogram().isPresent());

        assertEquals(expected, mv.getStrValue());
        assertEquals(expected, mv.stringValue().get());
    }

    public static void validateHistogram(Histogram expected, MetricValue mv) {
        assertNull(mv.getBoolValue());
        assertNull(mv.getIntValue());
        assertNull(mv.getFltValue());
        assertNull(mv.getStrValue());
        assertNotNull(mv.getHistValue());
        assertTrue(mv.asBool().isPresent());
        assertFalse(mv.value().isPresent());
        assertFalse(mv.stringValue().isPresent());

        assertEquals(expected, mv.getHistValue());
        assertEquals(expected, mv.histogram().get());
    }

    public static void validateEmpty(MetricValue mv) {
        assertNull(mv.getBoolValue());
        assertNull(mv.getIntValue());
        assertNull(mv.getFltValue());
        assertNull(mv.getStrValue());
        assertNull(mv.getHistValue());

        assertFalse(mv.asBool().isPresent());
        assertFalse(mv.value().isPresent());
        assertFalse(mv.stringValue().isPresent());
        assertFalse(mv.histogram().isPresent());

        assertEquals("(none)", mv.toString());
    }

    @Test
    public void empty() {
        validateEmpty(MetricValue.EMPTY);
    }

    @Test
    public void init_explicit() {
        validateNumber(true, 7, MetricValue.fromIntValue(7));
        validateNumber(false, 0, MetricValue.fromIntValue(0));
        validateNumber(true, 7D, MetricValue.fromDblValue(7D));
        validateNumber(false, 0D, MetricValue.fromDblValue(0D));
        validateHistogram(hist42, MetricValue.fromHistValue(hist42));
    }

    @Test
    public void init_string() {
        MetricValue mv = MetricValue.fromStrValue("foobar");
        validateString("foobar", mv);
    }

    @Test
    public void init_true() {
        MetricValue mv = MetricValue.fromBoolean(true);

        validateBoolean(true, mv);
        assertSame(MetricValue.TRUE, mv);
    }

    @Test
    public void init_false() {
        MetricValue mv = MetricValue.fromBoolean(false);

        validateBoolean(false, mv);
        assertSame(MetricValue.FALSE, mv);
    }

    @Test
    public void constructor() {
        validateBoolean(true, MetricValue.fromBoolean(true));
        validateBoolean(false, MetricValue.fromBoolean(false));
        validateNumber(true, 7, MetricValue.fromIntValue(SEVEN_LONG));
        validateNumber(true, 7D, MetricValue.fromDblValue(SEVEN_DOUBLE));
        validateString("foobar", MetricValue.fromStrValue("foobar"));
        validateEmpty(MetricValue.EMPTY);
    }

    @Test
    public void equality_none() {
        MetricValue x = MetricValue.EMPTY;
        MetricValue y = MetricValue.EMPTY;

        assertEquals(x, y);
        assertEquals(x.hashCode(), y.hashCode());
        assertEquals(0, x.compareTo(y));
    }

    @Test
    public void equality_bool() {
        MetricValue x = MetricValue.fromBoolean(true);
        MetricValue y = MetricValue.fromBoolean(true);

        assertEquals(x, y);
        assertEquals(x.hashCode(), y.hashCode());
        assertEquals(0, x.compareTo(y));
    }

    @Test
    public void equality_int() {
        MetricValue x = MetricValue.fromIntValue(5L);
        MetricValue y = MetricValue.fromIntValue(5L);

        assertEquals(x, y);
        assertEquals(x.hashCode(), y.hashCode());
        assertEquals(0, x.compareTo(y));
    }

    @Test
    public void equality_float() {
        MetricValue x = MetricValue.fromDblValue(5D);
        MetricValue y = MetricValue.fromDblValue(5D);

        assertEquals(x, y);
        assertEquals(x.hashCode(), y.hashCode());
        assertEquals(0, x.compareTo(y));
    }

    @Test
    public void equality_str() {
        MetricValue x = MetricValue.fromStrValue("baz");
        MetricValue y = MetricValue.fromStrValue("baz");

        assertEquals(x, y);
        assertEquals(x.hashCode(), y.hashCode());
        assertEquals(0, x.compareTo(y));
    }

    @Test
    public void equality_hist() {
        final MetricValue x = MetricValue.fromHistValue(new Histogram(Stream.of(
            new Histogram.RangeWithCount(new Histogram.Range(0,  1), 1),
            new Histogram.RangeWithCount(new Histogram.Range(7, 10), 3)
        )));
        final MetricValue y = MetricValue.fromHistValue(new Histogram(Stream.of(
            new Histogram.RangeWithCount(new Histogram.Range(0,  1), 1),
            new Histogram.RangeWithCount(new Histogram.Range(7,  8), 1),
            new Histogram.RangeWithCount(new Histogram.Range(8,  9), 1),
            new Histogram.RangeWithCount(new Histogram.Range(9, 10), 1)
        )));

        assertEquals(x, y);
        assertEquals(x.hashCode(), y.hashCode());
        assertEquals(0, x.compareTo(y));
    }

    @Test
    public void inequality_bool() {
        MetricValue x = MetricValue.fromBoolean(true);
        MetricValue y = MetricValue.fromBoolean(false);

        assertNotEquals(x, y);
        assertTrue(x.compareTo(y) > 0);
        assertTrue(y.compareTo(x) < 0);
    }

    @Test
    public void inequality_int() {
        MetricValue x = MetricValue.fromIntValue(5L);
        MetricValue y = MetricValue.fromIntValue(4L);

        assertNotEquals(x, y);
        assertTrue(x.compareTo(y) > 0);
        assertTrue(y.compareTo(x) < 0);
    }

    @Test
    public void inequality_float() {
        MetricValue x = MetricValue.fromDblValue(5D);
        MetricValue y = MetricValue.fromDblValue(4D);

        assertNotEquals(x, y);
        assertTrue(x.compareTo(y) > 0);
        assertTrue(y.compareTo(x) < 0);
    }

    @Test
    public void inequality_str() {
        MetricValue x = MetricValue.fromStrValue("foo");
        MetricValue y = MetricValue.fromStrValue("baz");

        assertNotEquals(x, y);
        assertTrue(x.compareTo(y) > 0);
        assertTrue(y.compareTo(x) < 0);
    }

    @Test
    public void inequality_hist() {
        MetricValue x = MetricValue.fromHistValue(hist42);
        MetricValue y = MetricValue.fromHistValue(hist24);

        assertNotEquals(x, y);
        assertTrue(x.compareTo(y) > 0);
        assertTrue(y.compareTo(x) < 0);
    }

    @Test
    public void inequality() {
        final MetricValue boolval = MetricValue.fromBoolean(true);
        final MetricValue intval = MetricValue.fromIntValue(1L);
        final MetricValue fltval = MetricValue.fromDblValue(1D);
        final MetricValue strval = MetricValue.fromStrValue("1");
        final MetricValue histval = MetricValue.fromHistValue(hist24);

        assertNotEquals(boolval, intval);
        assertNotEquals(boolval, fltval);
        assertNotEquals(boolval, strval);
        assertNotEquals(boolval, histval);

        assertNotEquals(intval, boolval);
        assertNotEquals(intval, fltval);
        assertNotEquals(intval, strval);
        assertNotEquals(intval, histval);

        assertNotEquals(fltval, boolval);
        assertNotEquals(fltval, intval);
        assertNotEquals(fltval, strval);
        assertNotEquals(fltval, histval);

        assertNotEquals(strval, boolval);
        assertNotEquals(strval, intval);
        assertNotEquals(strval, fltval);
        assertNotEquals(strval, histval);

        assertNotEquals(boolval, MetricValue.EMPTY);
        assertNotEquals(intval, MetricValue.EMPTY);
        assertNotEquals(fltval, MetricValue.EMPTY);
        assertNotEquals(strval, MetricValue.EMPTY);
        assertNotEquals(histval, MetricValue.EMPTY);
    }

    @Test
    public void compare() {
        final MetricValue boolval = MetricValue.fromBoolean(true);
        final MetricValue intval = MetricValue.fromIntValue(1L);
        final MetricValue fltval = MetricValue.fromDblValue(1D);
        final MetricValue strval = MetricValue.fromStrValue("1");
        final MetricValue histval = MetricValue.fromHistValue(hist24);

        assertTrue(boolval.compareTo(histval) < 0);
        assertTrue(boolval.compareTo(intval) > 0);
        assertTrue(boolval.compareTo(fltval) > 0);
        assertTrue(boolval.compareTo(strval) > 0);

        assertTrue(intval.compareTo(histval) < 0);
        assertTrue(intval.compareTo(boolval) < 0);
        assertTrue(intval.compareTo(fltval) > 0);
        assertTrue(intval.compareTo(strval) > 0);

        assertTrue(fltval.compareTo(histval) < 0);
        assertTrue(fltval.compareTo(boolval) < 0);
        assertTrue(fltval.compareTo(intval) < 0);
        assertTrue(fltval.compareTo(strval) > 0);

        assertTrue(strval.compareTo(histval) < 0);
        assertTrue(strval.compareTo(boolval) < 0);
        assertTrue(strval.compareTo(intval) < 0);
        assertTrue(strval.compareTo(fltval) < 0);

        assertTrue(histval.compareTo(MetricValue.EMPTY) > 0);
        assertTrue(boolval.compareTo(MetricValue.EMPTY) > 0);
        assertTrue(intval.compareTo(MetricValue.EMPTY) > 0);
        assertTrue(fltval.compareTo(MetricValue.EMPTY) > 0);
        assertTrue(strval.compareTo(MetricValue.EMPTY) > 0);

        assertTrue(MetricValue.EMPTY.compareTo(histval) < 0);
        assertTrue(MetricValue.EMPTY.compareTo(boolval) < 0);
        assertTrue(MetricValue.EMPTY.compareTo(intval) < 0);
        assertTrue(MetricValue.EMPTY.compareTo(fltval) < 0);
        assertTrue(MetricValue.EMPTY.compareTo(strval) < 0);
    }

    @Test
    public void create_number() {
        assertEquals(MetricValue.EMPTY, MetricValue.fromNumberValue(null));

        validateNumber(true, SEVEN_BYTE, MetricValue.fromNumberValue(SEVEN_BYTE));
        validateNumber(true, SEVEN_SHORT, MetricValue.fromNumberValue(SEVEN_SHORT));
        validateNumber(true, SEVEN_INT, MetricValue.fromNumberValue(SEVEN_INT));
        validateNumber(true, SEVEN_LONG, MetricValue.fromNumberValue(SEVEN_LONG));
        validateNumber(true, SEVEN_FLOAT, MetricValue.fromNumberValue(SEVEN_FLOAT));
        validateNumber(true, SEVEN_DOUBLE, MetricValue.fromNumberValue(SEVEN_DOUBLE));
    }

    @Test
    public void presence() {
        final MetricValue empty = MetricValue.EMPTY;
        final MetricValue boolval = MetricValue.fromBoolean(true);
        final MetricValue intval = MetricValue.fromIntValue(1L);
        final MetricValue fltval = MetricValue.fromDblValue(1D);
        final MetricValue strval = MetricValue.fromStrValue("1");
        final MetricValue histval = MetricValue.fromHistValue(hist42);

        assertFalse(empty.isPresent());
        assertTrue(boolval.isPresent());
        assertTrue(intval.isPresent());
        assertTrue(fltval.isPresent());
        assertTrue(strval.isPresent());
        assertTrue(histval.isPresent());
    }

    @Test
    public void asString() {
        final MetricValue empty = MetricValue.EMPTY;
        final MetricValue boolval = MetricValue.fromBoolean(true);
        final MetricValue intval = MetricValue.fromIntValue(1L);
        final MetricValue fltval = MetricValue.fromDblValue(1D);
        final MetricValue strval = MetricValue.fromStrValue("1");
        final MetricValue histval = MetricValue.fromHistValue(hist42);

        assertEquals(Optional.empty(), empty.asString());
        assertEquals(Optional.of("true"), boolval.asString());
        assertEquals(Optional.of("1"), intval.asString());
        assertEquals(Optional.of(Double.toString(1)), fltval.asString());
        assertEquals(Optional.of("1"), strval.asString());
        assertEquals(Optional.empty(), histval.asString());
    }

    @Test
    public void asBool() {
        final MetricValue empty = MetricValue.EMPTY;
        final MetricValue boolval = MetricValue.fromBoolean(true);
        final MetricValue intval = MetricValue.fromIntValue(1L);
        final MetricValue fltval = MetricValue.fromDblValue(1D);
        final MetricValue strval = MetricValue.fromStrValue("1");
        final MetricValue histval = MetricValue.fromHistValue(hist42);

        assertEquals(Optional.empty(), empty.asBool());
        assertEquals(Optional.of(Boolean.TRUE), boolval.asBool());
        assertEquals(Optional.of(Boolean.TRUE), intval.asBool());
        assertEquals(Optional.of(Boolean.TRUE), fltval.asBool());
        assertEquals(Optional.empty(), strval.asBool());
        assertEquals(Optional.of(Boolean.TRUE), histval.asBool());
    }

    @Test
    public void asBoolFalse() {
        final MetricValue empty = MetricValue.EMPTY;
        final MetricValue boolval = MetricValue.fromBoolean(false);
        final MetricValue intval = MetricValue.fromIntValue(0L);
        final MetricValue fltval = MetricValue.fromDblValue(0D);
        final MetricValue strval = MetricValue.fromStrValue("");
        final MetricValue histval = MetricValue.fromHistValue(new Histogram());

        assertEquals(Optional.empty(), empty.asBool());
        assertEquals(Optional.of(Boolean.FALSE), boolval.asBool());
        assertEquals(Optional.of(Boolean.FALSE), intval.asBool());
        assertEquals(Optional.of(Boolean.FALSE), fltval.asBool());
        assertEquals(Optional.empty(), strval.asBool());
        assertEquals(Optional.of(Boolean.FALSE), histval.asBool());
    }

    @Test
    public void configString() {
        final MetricValue empty = MetricValue.EMPTY;
        final MetricValue boolval = MetricValue.fromBoolean(true);
        final MetricValue intval = MetricValue.fromIntValue(1L);
        final MetricValue fltval = MetricValue.fromDblValue(1D);
        final MetricValue strval = MetricValue.fromStrValue("1 \u0700 \37 \n \f \r \"\'");

        assertNull(empty.configString());
        assertEquals("true", boolval.configString());
        assertEquals("1", intval.configString());
        assertEquals(Double.toString(1), fltval.configString());
        assertEquals("\"1 \\u0700 \\037 \\n \\f \\r \\\"'\"", strval.configString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void unrecognizedNumberType() {
        MetricValue.fromNumberValue(new Number() {
            @Override
            public int intValue() {
                fail("May not invoke extraction methods on unrecognized number type.");
                return 0;
            }

            @Override
            public long longValue() {
                fail("May not invoke extraction methods on unrecognized number type.");
                return 0;
            }

            @Override
            public float floatValue() {
                fail("May not invoke extraction methods on unrecognized number type.");
                return 0;
            }

            @Override
            public double doubleValue() {
                fail("May not invoke extraction methods on unrecognized number type.");
                return 0;
            }
        });
    }

    @Test
    public void object_inequality() {
        assertFalse(MetricValue.EMPTY.equals(null));
        assertFalse(MetricValue.EMPTY.equals(new Object()));
        assertFalse(MetricValue.fromBoolean(true).equals(null));
        assertFalse(MetricValue.fromBoolean(true).equals(new Object()));
        assertFalse(MetricValue.fromIntValue(1L).equals(null));
        assertFalse(MetricValue.fromIntValue(1L).equals(new Object()));
        assertFalse(MetricValue.fromDblValue(1D).equals(null));
        assertFalse(MetricValue.fromDblValue(1D).equals(new Object()));
        assertFalse(MetricValue.fromStrValue("1").equals(null));
        assertFalse(MetricValue.fromStrValue("1").equals(new Object()));
        assertFalse(MetricValue.fromHistValue(hist42).equals(null));
        assertFalse(MetricValue.fromHistValue(hist42).equals(new Object()));
    }
}
