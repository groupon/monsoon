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
package com.groupon.lex.metrics.timeseries;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import static java.util.Collections.EMPTY_MAP;
import static java.util.Collections.singletonMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class MutableTimeSeriesValueTest {
    private static final MetricName metric_key = new MetricName("value");
    private static final DateTime t0 = DateTime.now(DateTimeZone.UTC);
    private static final GroupName group_name = new GroupName("foo", "bar");
    private static final Map<MetricName, MetricValue> values = singletonMap(metric_key, MetricValue.fromIntValue(7));

    @Test
    public void constructor() {
        TimeSeriesValue tsv = new MutableTimeSeriesValue(t0, group_name, values);

        assertEquals(t0, tsv.getTimestamp());
        assertEquals(group_name, tsv.getGroup());
        assertEquals(new HashMap<>(values), new HashMap<>(tsv.getMetrics()));
        assertEquals(Optional.of(MetricValue.fromIntValue(7)), tsv.findMetric(metric_key));
    }

    @Test
    public void constructor_from_stream() {
        TimeSeriesValue tsv = new MutableTimeSeriesValue(t0, group_name, Stream.of(new Object()), (xx) -> metric_key, (xx) -> MetricValue.fromIntValue(7));

        assertEquals(t0, tsv.getTimestamp());
        assertEquals(group_name, tsv.getGroup());
        assertEquals(new HashMap<>(values), new HashMap<>(tsv.getMetrics()));
        assertEquals(Optional.of(MetricValue.fromIntValue(7)), tsv.findMetric(metric_key));
    }

    @Test
    public void equality() {
        TimeSeriesValue x = new MutableTimeSeriesValue(t0, group_name, values);
        TimeSeriesValue y = new MutableTimeSeriesValue(t0, group_name, Stream.of(new Object()), (xx) -> metric_key, (xx) -> MetricValue.fromIntValue(7));

        assertTrue(x.hashCode() == y.hashCode());
        assertTrue(x.equals(y));
    }

    @Test
    public void inequality() {
        TimeSeriesValue base = new MutableTimeSeriesValue(t0, group_name, values);
        TimeSeriesValue a = new MutableTimeSeriesValue(t0, group_name, EMPTY_MAP);
        TimeSeriesValue b = new MutableTimeSeriesValue(t0, new GroupName("abracadabra"), values);
        TimeSeriesValue c = new MutableTimeSeriesValue(t0.minus(Duration.standardSeconds(1)), group_name, values);

        assertFalse(base.equals(a));
        assertFalse(base.equals(b));
        assertFalse(base.equals(c));
    }

    @Test
    public void map_is_not_shared() {
        Map<MetricName, MetricValue> local = new HashMap<>(values);
        TimeSeriesValue tsv = new MutableTimeSeriesValue(t0, group_name, local);
        local.remove(metric_key);  // Modifying local may not affect the TimeSeriesValue.

        assertEquals(values, new HashMap<>(tsv.getMetrics()));
    }

    @Test
    public void equals_across_types() {
        TimeSeriesValue tsv = new MutableTimeSeriesValue(t0, group_name, values);

        assertFalse(tsv.equals(null));
        assertFalse(tsv.equals(new Object()));
    }
}
