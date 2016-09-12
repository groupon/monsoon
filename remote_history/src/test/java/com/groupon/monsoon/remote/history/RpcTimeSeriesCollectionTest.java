/*
 * Copyright (c) 2016, Ariane van der Steldt
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
package com.groupon.monsoon.remote.history;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
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
public class RpcTimeSeriesCollectionTest {
    private TimeSeriesValue tsv;
    private RpcTimeSeriesCollection tsc, copy;
    private DateTime ts;

    @Before
    public void setup() {
        ts = DateTime.now(DateTimeZone.UTC);
        tsv = new MutableTimeSeriesValue(ts, GroupName.valueOf(SimpleGroupPath.valueOf("foo", "bar")), singletonMap(MetricName.valueOf("x"), MetricValue.fromStrValue("y")));
        tsc = new RpcTimeSeriesCollection(ts, Stream.of(tsv));
        copy = EncDec.decodeTSC(EncDec.encodeTSC(tsc));
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
        assertFalse(tsc.equals(new RpcTimeSeriesCollection(ts, Stream.empty())));
        assertFalse(tsc.equals(new RpcTimeSeriesCollection(ts.plusMillis(500), Stream.empty())));

        assertFalse(copy.equals(null));
        assertFalse(copy.equals(new Object()));
        assertFalse(copy.equals(new RpcTimeSeriesCollection(ts, Stream.empty())));
        assertFalse(copy.equals(new RpcTimeSeriesCollection(ts.plusMillis(500), Stream.empty())));
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
        tsc.addMetrics(tsv.getGroup(), singletonMap(MetricName.valueOf("y"), MetricValue.fromIntValue(17)));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void add_metric() {
        tsc.addMetric(tsv.getGroup(), MetricName.valueOf("y"), MetricValue.fromIntValue(17));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void rename_group() {
        tsc.renameGroup(tsv.getGroup(), GroupName.valueOf(SimpleGroupPath.valueOf("x", "y")));
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
        assertThat(tsc.getTSValues(), contains(tsv));
        assertThat(copy.getTSValues(), contains(tsv));
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

        assertTrue(new RpcTimeSeriesCollection(ts, Stream.empty()).isEmpty());
        assertTrue(EncDec.decodeTSC(EncDec.encodeTSC(new RpcTimeSeriesCollection(ts, Stream.empty()))).isEmpty());
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
