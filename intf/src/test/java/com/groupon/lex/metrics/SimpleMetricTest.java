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

import org.hamcrest.Matchers;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class SimpleMetricTest {
    @Test
    public void constructor_number() {
        Metric m = new SimpleMetric(MetricName.valueOf("foobar"), (short)7);

        assertEquals(MetricName.valueOf("foobar"), m.getName());
        assertNotNull(m.getValue());
        MetricValueTest.validateNumber(true, 7, m.getValue());
    }

    @Test
    public void constructor_string() {
        Metric m = new SimpleMetric(MetricName.valueOf("foobar"), "chocoladevla");

        assertEquals(MetricName.valueOf("foobar"), m.getName());
        assertNotNull(m.getValue());
        MetricValueTest.validateString("chocoladevla", m.getValue());
    }

    @Test
    public void constructor_bool() {
        Metric m = new SimpleMetric(MetricName.valueOf("foobar"), true);

        assertEquals(MetricName.valueOf("foobar"), m.getName());
        assertNotNull(m.getValue());
        MetricValueTest.validateBoolean(true, m.getValue());
    }

    @Test
    public void constructor_metric() {
        Metric m = new SimpleMetric(MetricName.valueOf("foobar"), MetricValue.fromNumberValue(9000));

        assertEquals(MetricName.valueOf("foobar"), m.getName());
        assertNotNull(m.getValue());
        MetricValueTest.validateNumber(true, 9000, m.getValue());
    }

    @Test
    public void constructor_empty() {
        Metric m = new SimpleMetric(MetricName.valueOf("foobar"), MetricValue.EMPTY);

        assertEquals(MetricName.valueOf("foobar"), m.getName());
        assertNotNull(m.getValue());
        MetricValueTest.validateEmpty(m.getValue());
    }

    @Test
    public void to_string() {
        Metric m = new SimpleMetric(MetricName.valueOf("foobar"), MetricValue.fromIntValue(19));

        assertThat(m.toString(), Matchers.allOf(containsString("foobar"), containsString("19")));
    }

    @Test
    public void equality() {
        Metric m0 = new SimpleMetric(MetricName.valueOf("foobar"), MetricValue.fromIntValue(19));
        Metric m1 = new SimpleMetric(MetricName.valueOf("foobar"), MetricValue.fromIntValue(19));

        assertEquals(m0, m1);
        assertEquals(m0.hashCode(), m1.hashCode());
    }

    @Test
    public void inequality() {
        Metric m0 = new SimpleMetric(MetricName.valueOf("foobar"), MetricValue.fromIntValue(17));
        Metric m1 = new SimpleMetric(MetricName.valueOf("foobar"), MetricValue.fromIntValue(19));
        Metric m2 = new SimpleMetric(MetricName.valueOf("fizzbuzz"), MetricValue.fromIntValue(19));

        assertNotEquals(m0, m1);
        assertNotEquals(m0, m2);

        assertNotEquals(m1, m0);
        assertNotEquals(m1, m2);

        assertNotEquals(m2, m0);
        assertNotEquals(m2, m1);
    }

    @Test
    public void equal_across_types() {
        Metric m = new SimpleMetric(MetricName.valueOf("foobar"), MetricValue.fromIntValue(19));

        assertFalse(m.equals(null));
        assertFalse(m.equals(new Object()));
    }
}
