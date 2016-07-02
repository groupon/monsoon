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
import com.groupon.lex.metrics.SimpleGroupPath;
import static java.util.Collections.EMPTY_MAP;
import static java.util.Collections.singletonMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class MutableTimeSeriesCollectionTest {
    private final static MetricName ts_metric_value__name = new MetricName("magic word");
    private final static MetricValue ts_metric_value__value = MetricValue.fromStrValue("abracadabra");
    private static final DateTime t0 = new DateTime(2015, 10, 21, 11, 28, 7, DateTimeZone.UTC);
    private static final DateTime t0_ts = t0.minus(Duration.standardSeconds(5));
    private static final SimpleGroupPath group_name = new SimpleGroupPath("com", "groupon", "lex", "jmx-monitord", "Awesomoium");
    private static final MutableTimeSeriesValue ts_value = new MutableTimeSeriesValue(t0_ts, new GroupName(group_name, EMPTY_MAP), singletonMap(ts_metric_value__name, ts_metric_value__value));
    private static final MutableTimeSeriesValue absent_value = new MutableTimeSeriesValue(t0, new GroupName("not", "here"), singletonMap(new MetricName("foo"), MetricValue.TRUE));

    @Test
    public void constructor() {
        TimeSeriesCollection ts_data = new MutableTimeSeriesCollection(t0);

        assertEquals(t0, ts_data.getTimestamp());
        assertTrue(ts_data.getTSValues().isEmpty());
        assertTrue(ts_data.getGroups().isEmpty());
        assertTrue(ts_data.isEmpty());
    }

    @Test
    public void constructor_with_value() {
        TimeSeriesCollection ts_data = new MutableTimeSeriesCollection(t0, Stream.of(ts_value));

        assertEquals(t0, ts_data.getTimestamp());
        assertThat(ts_data.getTSValues().stream()
                        .collect(Collectors.toList()),
                hasItem(ts_value));
        assertThat(ts_data.getGroups(),
                hasItem(new GroupName(group_name, EMPTY_MAP)));
        assertFalse(ts_data.isEmpty());
    }

    @Test
    public void empty() {
        DateTime t_before = DateTime.now(DateTimeZone.UTC);
        TimeSeriesCollection empty = new MutableTimeSeriesCollection();
        DateTime t_after = DateTime.now(DateTimeZone.UTC);

        assertFalse(t_before.isAfter(empty.getTimestamp()));
        assertFalse(t_after.isBefore(empty.getTimestamp()));
        assertTrue(empty.getTSValues().isEmpty());
        assertTrue(empty.isEmpty());
    }

    @Test
    public void empty_timestamp() {
        TimeSeriesCollection empty = new MutableTimeSeriesCollection(t0);

        assertEquals(t0, empty.getTimestamp());
        assertTrue(empty.getTSValues().isEmpty());
        assertTrue(empty.isEmpty());
    }

    @Test
    public void lookup() {
        TimeSeriesCollection ts_data = new MutableTimeSeriesCollection(t0, Stream.of(ts_value));

        assertThat(ts_data.getTSValue(group_name).stream().collect(Collectors.toList()),
                hasItem(ts_value));
        assertTrue(ts_data.getTSValue(new SimpleGroupPath("can't", "touch", "this")).stream().collect(Collectors.toList()).isEmpty());
    }

    @Test
    public void add_metric_to_nonexistant_group() {
        TimeSeriesCollection ts_data = new MutableTimeSeriesCollection(t0, Stream.of(ts_value));
        ts_data.add(absent_value);

        assertThat(ts_data.getTSValue(absent_value.getGroup().getPath()).stream().collect(Collectors.toList()),
                hasItem(absent_value));
        assertThat(ts_data.getTSValue(group_name).stream().collect(Collectors.toList()),
                hasItem(ts_value));
    }

    @Test
    public void overwrite_existing_group() {
        TimeSeriesCollection ts_data = new MutableTimeSeriesCollection(t0, Stream.of(ts_value));
        ts_data.add(new MutableTimeSeriesValue(t0, new GroupName(group_name), singletonMap(new MetricName("foo"), MetricValue.fromStrValue("bar"))));

        assertThat(ts_data.getTSValue(group_name).stream().collect(Collectors.toList()),
                hasItem(new MutableTimeSeriesValue(t0, new GroupName(group_name), singletonMap(new MetricName("foo"), MetricValue.fromStrValue("bar")))));
    }

    @Test
    public void add_metric() {
        TimeSeriesCollection ts_data = new MutableTimeSeriesCollection(t0, Stream.of(ts_value));
        ts_data.addMetric(new GroupName(group_name), new MetricName("foo"), MetricValue.fromStrValue("bar"));
        TimeSeriesValueSet group = ts_data.getTSValue(group_name);

        assertTrue(!group.isEmpty());
        assertThat(group.findMetric(new MetricName("foo")).streamValues()
                        .collect(Collectors.toList()),
                hasItem(MetricValue.fromStrValue("bar")));
        assertThat(group.findMetric(ts_metric_value__name).streamValues()
                        .collect(Collectors.toList()),
                hasItem(ts_metric_value__value));
    }

    @Test
    public void rename_group() {
        TimeSeriesCollection ts_data = new MutableTimeSeriesCollection(t0, Stream.of(ts_value));
        final TimeSeriesValue EXPECT_TS_AFTER_RENAME = new MutableTimeSeriesValue(t0_ts, new GroupName("post", "rename"), singletonMap(ts_metric_value__name, ts_metric_value__value));

        ts_data.renameGroup(new GroupName(group_name), new GroupName("post", "rename"));

        assertEquals(t0, ts_data.getTimestamp());
        assertThat(ts_data.getTSValues().stream()
                        .collect(Collectors.toList()),
                hasItem(EXPECT_TS_AFTER_RENAME));
        assertThat(ts_data.getGroups(),
                hasItem(new GroupName("post", "rename")));
        assertFalse(ts_data.isEmpty());
    }

    @Test
    public void hashcode() {
        TimeSeriesCollection ts_data_1 = new MutableTimeSeriesCollection(t0, Stream.of(ts_value));
        TimeSeriesCollection ts_data_2 = new MutableTimeSeriesCollection(t0, Stream.of(ts_value));

        assertEquals(ts_data_1.hashCode(), ts_data_2.hashCode());
    }

    @Test
    public void inequality() {
        TimeSeriesCollection ts_data = new MutableTimeSeriesCollection(t0, Stream.of(ts_value));
        TimeSeriesCollection other_time = new MutableTimeSeriesCollection(t0_ts, Stream.of(ts_value));
        TimeSeriesCollection other_data = new MutableTimeSeriesCollection(t0, Stream.of(absent_value));

        assertNotEquals(ts_data, other_time);
        assertNotEquals(ts_data, other_data);
        assertFalse(ts_data.equals(null));
        assertFalse(ts_data.equals(new Object()));
    }

    @Test
    public void clone_tsdata() {
        TimeSeriesCollection ts_data = new MutableTimeSeriesCollection(t0, Stream.of(ts_value));
        TimeSeriesCollection clone = ts_data.clone();

        assertNotSame(ts_data, clone);
        assertEquals(ts_data, clone);

        // Clone and original are indepent with respect to modifications.
        ts_data.add(absent_value);
        assertThat(ts_data.getGroups(), hasItem(absent_value.getGroup()));
        assertThat(clone.getGroups(), not(hasItem(absent_value.getGroup())));
        assertNotEquals(ts_data, clone);
    }
}
