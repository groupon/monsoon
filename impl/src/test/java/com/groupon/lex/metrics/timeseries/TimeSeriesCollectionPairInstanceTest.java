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
import java.util.HashSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static org.hamcrest.CoreMatchers.hasItem;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class TimeSeriesCollectionPairInstanceTest {
    private MutableTimeSeriesValue present_value(DateTime ts, int val) {
        return new MutableTimeSeriesValue(ts, new GroupName("present"), singletonMap(new MetricName("value"), MetricValue.fromIntValue(val)));
    }

    private final DateTime date2 = new DateTime(2011, 1, 1, 0, 0, 0, DateTimeZone.UTC);
    private final DateTime date1 = new DateTime(2011, 1, 1, 0, 1, 0, DateTimeZone.UTC);
    private final DateTime date0 = new DateTime(2011, 1, 1, 0, 2, 0, DateTimeZone.UTC);
    private final MutableTimeSeriesValue pv2 = present_value(date2, 2);
    private final MutableTimeSeriesValue date2_data = new MutableTimeSeriesValue(date2, new GroupName("date2"), EMPTY_MAP);
    private final MutableTimeSeriesValue pv1 = present_value(date1, 1);
    private final MutableTimeSeriesValue date1_data = new MutableTimeSeriesValue(date1, new GroupName("date1"), EMPTY_MAP);
    private final MutableTimeSeriesValue pv0 = present_value(date0, 0);
    private final MutableTimeSeriesValue date0_data = new MutableTimeSeriesValue(date0, new GroupName("date0"), EMPTY_MAP);
    private final MutableTimeSeriesCollection collection2 = new MutableTimeSeriesCollection(date2,
            Stream.concat(Stream.of(date2_data), Stream.of(pv2.clone())));
    private final MutableTimeSeriesCollection collection1 = new MutableTimeSeriesCollection(date1,
            Stream.concat(Stream.of(date1_data), Stream.of(pv1.clone())));
    private final MutableTimeSeriesCollection collection0 = new MutableTimeSeriesCollection(date0,
            Stream.concat(Stream.of(date0_data), Stream.of(pv0.clone())));

    @Test
    public void constructor() {
        DateTime before = DateTime.now(DateTimeZone.UTC);
        TimeSeriesCollectionPair ts_data = new TimeSeriesCollectionPairInstance();
        DateTime after = DateTime.now(DateTimeZone.UTC);

        assertNotNull(ts_data.getPreviousCollection());
        assertNotNull(ts_data.getCurrentCollection());

        assertFalse("before <= ts_data.previousCollection().getTimestamp()",
                before.isAfter(ts_data.getPreviousCollection().getTimestamp()));
        assertFalse("ts_data.currentCollection().getTimestamp() <= after",
                ts_data.getCurrentCollection().getTimestamp().isAfter(after));
        assertNotSame(ts_data.getPreviousCollection(), ts_data.getCurrentCollection());

        assertTrue(ts_data.getCurrentCollection().isEmpty());
        assertTrue(ts_data.getPreviousCollection().isEmpty());
    }

    @Test
    public void constructor_args() {
        TimeSeriesCollectionPairInstance ts_data = new TimeSeriesCollectionPairInstance();
        ts_data.startNewCycle(collection1.getTimestamp(), ExpressionLookBack.EMPTY);
        collection1.getData().values().forEach(ts_data.getCurrentCollection()::add);
        ts_data.startNewCycle(collection0.getTimestamp(), ExpressionLookBack.EMPTY);
        collection0.getData().values().forEach(ts_data.getCurrentCollection()::add);

        assertEquals(collection1.getTimestamp(), ts_data.getPreviousCollection().getTimestamp());
        assertEquals(new HashSet<>(collection1.getTSValues()), new HashSet<>(ts_data.getPreviousCollection().getTSValues()));
        assertEquals(collection0, ts_data.getCurrentCollection());
    }

    @Test
    public void lookup() {
        TimeSeriesCollectionPairInstance ts_data = new TimeSeriesCollectionPairInstance();
        ts_data.startNewCycle(collection1.getTimestamp(), ExpressionLookBack.EMPTY);
        collection1.getData().values().forEach(ts_data.getCurrentCollection()::add);
        ts_data.startNewCycle(collection0.getTimestamp(), ExpressionLookBack.EMPTY);
        collection0.getData().values().forEach(ts_data.getCurrentCollection()::add);
        TimeSeriesValueSet ts_delta = ts_data.getTSValue(new SimpleGroupPath("present"));

        assertThat(ts_delta.stream().collect(Collectors.toList()), hasItem(pv0));
    }

    @Test
    public void lookup_current_only() {
        TimeSeriesCollectionPairInstance ts_data = new TimeSeriesCollectionPairInstance();
        ts_data.startNewCycle(collection1.getTimestamp(), ExpressionLookBack.EMPTY);
        collection1.getData().values().forEach(ts_data.getCurrentCollection()::add);
        ts_data.startNewCycle(collection0.getTimestamp(), ExpressionLookBack.EMPTY);
        collection0.getData().values().forEach(ts_data.getCurrentCollection()::add);
        TimeSeriesValueSet ts_delta = ts_data.getTSValue(new SimpleGroupPath("date0"));

        assertThat(ts_delta.stream().collect(Collectors.toList()), hasItem(date0_data));
    }

    @Test
    public void lookup_past_only() {
        TimeSeriesCollectionPairInstance ts_data = new TimeSeriesCollectionPairInstance();
        ts_data.startNewCycle(collection1.getTimestamp(), ExpressionLookBack.EMPTY);
        collection1.getData().values().forEach(ts_data.getCurrentCollection()::add);
        ts_data.startNewCycle(collection0.getTimestamp(), ExpressionLookBack.EMPTY);
        collection0.getData().values().forEach(ts_data.getCurrentCollection()::add);
        TimeSeriesValueSet ts_delta = ts_data.getTSValue(new SimpleGroupPath("date1"));

        assertTrue(ts_delta.isEmpty());
    }

    @Test
    public void lookup_non_existent() {
        TimeSeriesCollectionPairInstance ts_data = new TimeSeriesCollectionPairInstance();
        ts_data.startNewCycle(collection1.getTimestamp(), ExpressionLookBack.EMPTY);
        collection1.getData().values().forEach(ts_data.getCurrentCollection()::add);
        ts_data.startNewCycle(collection0.getTimestamp(), ExpressionLookBack.EMPTY);
        collection0.getData().values().forEach(ts_data.getCurrentCollection()::add);
        TimeSeriesValueSet ts_delta = ts_data.getTSValue(new SimpleGroupPath("I", "don't", "exist"));

        assertTrue(ts_delta.isEmpty());
    }
}
