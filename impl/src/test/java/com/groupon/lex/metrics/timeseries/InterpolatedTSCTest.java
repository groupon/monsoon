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
import com.groupon.lex.metrics.Histogram;
import com.groupon.lex.metrics.Histogram.RangeWithCount;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import java.util.Arrays;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import java.util.Optional;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class InterpolatedTSCTest {
    @Mock
    private TimeSeriesValue pastValue, futureValue, presentValue;

    private static final GroupName TESTGROUP = GroupName.valueOf("test");
    private static final MetricName TESTMETRIC = MetricName.valueOf("metric");
    private final DateTime pastDate = new DateTime(1970, 1, 1, 0, 0, DateTimeZone.UTC);
    private final DateTime futureDate = new DateTime(1970, 1, 1, 1, 0, DateTimeZone.UTC);
    /**
     * One quarter after pastDate, three quarters before futureDate.
     */
    private final DateTime midDate = new DateTime(1970, 1, 1, 0, 15, DateTimeZone.UTC);
    private TimeSeriesCollection past, future;

    @Before
    public void setup() {
        when(pastValue.getGroup()).thenReturn(TESTGROUP);
        when(futureValue.getGroup()).thenReturn(TESTGROUP);
        when(presentValue.getGroup()).thenReturn(TESTGROUP);
        when(pastValue.getTags()).thenReturn(TESTGROUP.getTags());
        when(futureValue.getTags()).thenReturn(TESTGROUP.getTags());
        when(pastValue.getTimestamp()).thenReturn(pastDate);
        when(futureValue.getTimestamp()).thenReturn(futureDate);
        when(pastValue.findMetric(Mockito.any())).thenCallRealMethod();
        when(futureValue.findMetric(Mockito.any())).thenCallRealMethod();

        past = new BackRefTimeSeriesCollection(pastDate, singletonList(pastValue));
        future = new BackRefTimeSeriesCollection(futureDate, singletonList(futureValue));
    }

    @Test
    public void interpolateBoolean() {
        when(pastValue.getMetrics()).thenReturn(singletonMap(TESTMETRIC, MetricValue.fromBoolean(false)));
        when(futureValue.getMetrics()).thenReturn(singletonMap(TESTMETRIC, MetricValue.fromBoolean(true)));

        InterpolatedTSC interpolatedTSC = new InterpolatedTSC(new BackRefTimeSeriesCollection(midDate), singletonList(past), singletonList(future));

        assertTrue(interpolatedTSC.get(TESTGROUP).isPresent());
        assertEquals(TESTGROUP, interpolatedTSC.get(TESTGROUP).get().getGroup());
        assertEquals(midDate, interpolatedTSC.get(TESTGROUP).get().getTimestamp());
        assertEquals(Optional.of(MetricValue.fromDblValue(0.25)), interpolatedTSC.get(TESTGROUP).get().findMetric(TESTMETRIC));
    }

    @Test
    public void interpolateInteger() {
        when(pastValue.getMetrics()).thenReturn(singletonMap(TESTMETRIC, MetricValue.fromIntValue(4)));
        when(futureValue.getMetrics()).thenReturn(singletonMap(TESTMETRIC, MetricValue.fromIntValue(8)));

        InterpolatedTSC interpolatedTSC = new InterpolatedTSC(new BackRefTimeSeriesCollection(midDate), singletonList(past), singletonList(future));

        assertTrue(interpolatedTSC.get(TESTGROUP).isPresent());
        assertEquals(TESTGROUP, interpolatedTSC.get(TESTGROUP).get().getGroup());
        assertEquals(midDate, interpolatedTSC.get(TESTGROUP).get().getTimestamp());
        assertEquals(Optional.of(MetricValue.fromDblValue(5)), interpolatedTSC.get(TESTGROUP).get().findMetric(TESTMETRIC));
    }

    @Test
    public void interpolateFloatingPointValue() {
        when(pastValue.getMetrics()).thenReturn(singletonMap(TESTMETRIC, MetricValue.fromDblValue(4.9)));
        when(futureValue.getMetrics()).thenReturn(singletonMap(TESTMETRIC, MetricValue.fromDblValue(8.9)));

        InterpolatedTSC interpolatedTSC = new InterpolatedTSC(new BackRefTimeSeriesCollection(midDate), singletonList(past), singletonList(future));

        assertTrue(interpolatedTSC.get(TESTGROUP).isPresent());
        assertEquals(TESTGROUP, interpolatedTSC.get(TESTGROUP).get().getGroup());
        assertEquals(midDate, interpolatedTSC.get(TESTGROUP).get().getTimestamp());
        assertEquals(Optional.of(MetricValue.fromDblValue(5.9)), interpolatedTSC.get(TESTGROUP).get().findMetric(TESTMETRIC));
    }

    @Test
    public void interpolateString() {
        when(pastValue.getMetrics()).thenReturn(singletonMap(TESTMETRIC, MetricValue.fromStrValue("foo")));
        when(futureValue.getMetrics()).thenReturn(singletonMap(TESTMETRIC, MetricValue.fromStrValue("bar")));

        InterpolatedTSC interpolatedTSC = new InterpolatedTSC(new BackRefTimeSeriesCollection(midDate), singletonList(past), singletonList(future));

        assertTrue(interpolatedTSC.get(TESTGROUP).isPresent());
        assertEquals(TESTGROUP, interpolatedTSC.get(TESTGROUP).get().getGroup());
        assertEquals(midDate, interpolatedTSC.get(TESTGROUP).get().getTimestamp());
        assertEquals(Optional.of(MetricValue.fromStrValue("foo")), interpolatedTSC.get(TESTGROUP).get().findMetric(TESTMETRIC));
    }

    @Test
    public void interpolateHistogram() {
        when(pastValue.getMetrics()).thenReturn(singletonMap(TESTMETRIC, MetricValue.fromHistValue(new Histogram(new RangeWithCount(0, 10, 4)))));
        when(futureValue.getMetrics()).thenReturn(singletonMap(TESTMETRIC, MetricValue.fromHistValue(new Histogram(new RangeWithCount(0, 10, 8)))));

        InterpolatedTSC interpolatedTSC = new InterpolatedTSC(new BackRefTimeSeriesCollection(midDate), singletonList(past), singletonList(future));

        assertTrue(interpolatedTSC.get(TESTGROUP).isPresent());
        assertEquals(TESTGROUP, interpolatedTSC.get(TESTGROUP).get().getGroup());
        assertEquals(midDate, interpolatedTSC.get(TESTGROUP).get().getTimestamp());
        assertEquals(Optional.of(MetricValue.fromHistValue(new Histogram(new RangeWithCount(0, 10, 5)))), interpolatedTSC.get(TESTGROUP).get().findMetric(TESTMETRIC));
    }

    @Test
    public void namesFromBothArePresent() {
        BackRefTimeSeriesCollection present = new BackRefTimeSeriesCollection(midDate, singletonList(new MutableTimeSeriesValue(midDate, GroupName.valueOf("mid", "point"))));
        InterpolatedTSC interpolatedTSC = new InterpolatedTSC(present, singletonList(past), singletonList(future));

        assertThat(interpolatedTSC.getGroups(),
                containsInAnyOrder(TESTGROUP, GroupName.valueOf("mid", "point")));
        assertThat(interpolatedTSC.getGroupPaths(),
                containsInAnyOrder(TESTGROUP.getPath(), SimpleGroupPath.valueOf("mid", "point")));
    }

    @Test
    public void presentOverridesInterpolation() {
        BackRefTimeSeriesCollection present = new BackRefTimeSeriesCollection(midDate, singletonList(presentValue));
        InterpolatedTSC interpolatedTSC = new InterpolatedTSC(present, singletonList(past), singletonList(future));

        assertThat(interpolatedTSC.getGroups(),
                contains(TESTGROUP));
        assertThat(interpolatedTSC.getGroupPaths(),
                contains(TESTGROUP.getPath()));
        assertSame(presentValue, interpolatedTSC.get(TESTGROUP).get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsIfPresentTooOld() {
        new InterpolatedTSC(new BackRefTimeSeriesCollection(pastDate.minus(Duration.standardDays(1))), singletonList(past), singletonList(future));
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsIfPresentTooNew() {
        new InterpolatedTSC(new BackRefTimeSeriesCollection(futureDate.plus(Duration.standardDays(1))), singletonList(past), singletonList(future));
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsIfPastMisordered() {
        new InterpolatedTSC(new BackRefTimeSeriesCollection(futureDate.plus(Duration.standardDays(1))), Arrays.asList(past, future), EMPTY_LIST);
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsIfFutureMisordered() {
        new InterpolatedTSC(new BackRefTimeSeriesCollection(pastDate.minus(Duration.standardDays(1))), EMPTY_LIST, Arrays.asList(future, past));
    }

    @Test
    public void equality() {
        when(pastValue.getMetrics()).thenReturn(singletonMap(TESTMETRIC, MetricValue.fromIntValue(4)));
        when(futureValue.getMetrics()).thenReturn(singletonMap(TESTMETRIC, MetricValue.fromIntValue(8)));

        final BackRefTimeSeriesCollection expected = new BackRefTimeSeriesCollection(midDate, singletonList(new MutableTimeSeriesValue(midDate, TESTGROUP, singletonMap(TESTMETRIC, MetricValue.fromDblValue(5)))));
        InterpolatedTSC interpolatedTSC = new InterpolatedTSC(new BackRefTimeSeriesCollection(midDate), singletonList(past), singletonList(future));

        assertEquals(expected.hashCode(), interpolatedTSC.hashCode());
        assertTrue(interpolatedTSC.equals(expected));
        assertTrue(expected.equals(interpolatedTSC));
    }
}
