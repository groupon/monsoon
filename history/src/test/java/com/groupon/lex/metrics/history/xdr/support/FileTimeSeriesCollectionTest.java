/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.groupon.lex.metrics.history.xdr.support;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.history.v0.xdr.FromXdr;
import com.groupon.lex.metrics.history.v0.xdr.ToXdr;
import com.groupon.lex.metrics.timeseries.MutableTimeSeriesValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesValueSet;
import static java.util.Collections.singletonMap;
import java.util.Optional;
import java.util.stream.Stream;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.sameInstance;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class FileTimeSeriesCollectionTest {
    private TimeSeriesValue tsv;
    private FileTimeSeriesCollection tsc, copy;
    private DateTime ts;

    @Before
    public void setup() {
        ts = DateTime.now(DateTimeZone.UTC);
        tsv = new MutableTimeSeriesValue(ts, new GroupName(new SimpleGroupPath("foo", "bar")), singletonMap(new MetricName("x"), MetricValue.fromStrValue("y")));
        tsc = new FileTimeSeriesCollection(ts, Stream.of(tsv));
        copy = FromXdr.datapoints(ToXdr.datapoints(tsc));
    }

    @Test
    public void serialize_deserialize() {
        assertEquals(tsc, copy);
        assertEquals(ts, copy.getTimestamp());
    }

    @Test
    public void equality() {
        assertTrue(copy.equals(tsc));
        assertTrue(tsc.equals(copy));
        assertEquals(tsc.hashCode(), copy.hashCode());
    }

    @Test
    public void inequality() {
        assertFalse(tsc.equals(null));
        assertFalse(tsc.equals(new Object()));
        assertFalse(tsc.equals(new FileTimeSeriesCollection(ts, Stream.empty())));
        assertFalse(tsc.equals(new FileTimeSeriesCollection(ts.plusMillis(500), Stream.empty())));

        assertFalse(copy.equals(null));
        assertFalse(copy.equals(new Object()));
        assertFalse(copy.equals(new FileTimeSeriesCollection(ts, Stream.empty())));
        assertFalse(copy.equals(new FileTimeSeriesCollection(ts.plusMillis(500), Stream.empty())));
    }

    @Test
    public void get_groups() {
        assertThat(tsc.getGroups(), contains(tsv.getGroup()));
        assertThat(copy.getGroups(), contains(tsv.getGroup()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void add_tsc() {
        tsc.add(tsv);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void add_metrics() {
        tsc.addMetrics(tsv.getGroup(), singletonMap(new MetricName("y"), MetricValue.fromIntValue(17)));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void add_metric() {
        tsc.addMetric(tsv.getGroup(), new MetricName("y"), MetricValue.fromIntValue(17));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void rename_group() {
        tsc.renameGroup(tsv.getGroup(), new GroupName(new SimpleGroupPath("x", "y")));
    }

    @Test
    public void get_group_paths() {
        assertThat(tsc.getGroupPaths(), contains(tsv.getGroup().getPath()));
        assertThat(copy.getGroupPaths(), contains(tsv.getGroup().getPath()));
    }

    @Test
    public void get_tsvalue_by_path() {
        assertThat(tsc.getTSValue(tsv.getGroup().getPath()).asMap(), hasEntry(equalTo(tsv.getGroup().getPath()), contains(tsv)));
        assertThat(copy.getTSValue(tsv.getGroup().getPath()).asMap(), hasEntry(equalTo(tsv.getGroup().getPath()), contains(tsv)));
    }

    @Test
    public void get_tsvalues() {
        assertThat(tsc.getTSValuesAsMap(), hasEntry(equalTo(tsv.getGroup().getPath()), contains(tsv)));
        assertThat(copy.getTSValuesAsMap(), hasEntry(equalTo(tsv.getGroup().getPath()), contains(tsv)));
    }

    @Test
    public void get_tsdelta_by_name() {
        Optional<TimeSeriesValueSet> tsc_found = tsc.getTSDeltaByName(tsv.getGroup());
        Optional<TimeSeriesValueSet> copy_found = copy.getTSDeltaByName(tsv.getGroup());

        assertTrue(tsc_found.isPresent());
        assertThat(tsc_found.get().asMap(), hasEntry(equalTo(tsv.getGroup().getPath()), contains(sameInstance(tsv))));
        assertTrue(copy_found.isPresent());
        assertThat(copy_found.get().asMap(), hasEntry(equalTo(tsv.getGroup().getPath()), contains(equalTo(tsv))));
    }

    @Test
    public void is_empty() {
        assertFalse(tsc.isEmpty());
        assertFalse(copy.isEmpty());

        assertTrue(new FileTimeSeriesCollection(ts, Stream.empty()).isEmpty());
        assertTrue(FromXdr.datapoints(ToXdr.datapoints(new FileTimeSeriesCollection(ts, Stream.empty()))).isEmpty());
    }

    @Test
    public void clone_is_similar() {
        assertEquals(tsc, tsc.clone());
        assertEquals(copy, copy.clone());
    }

    @Test
    public void to_string() {
        assertThat(tsc.toString(), allOf(containsString(ts.toString()), containsString(tsv.getGroup().toString())));
        assertThat(copy.toString(), allOf(containsString(ts.toString()), containsString(tsv.getGroup().toString())));
    }
}
