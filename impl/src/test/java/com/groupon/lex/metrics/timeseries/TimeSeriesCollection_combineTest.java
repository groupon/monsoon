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
import java.util.Arrays;
import java.util.Collection;
import static java.util.Collections.singletonMap;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hamcrest.CoreMatchers;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class TimeSeriesCollection_combineTest {
    private DateTime t0, t1;
    private TimeSeriesValue tsv_old_keep, tsv_old_overwrite, tsv_new, tsv_new_overwrite;
    private Collection<TimeSeriesValue> expected;
    private MutableTimeSeriesCollection ts_data_0, ts_data_1;

    private static Set<TimeSeriesValue> set(Collection<TimeSeriesValue> data) {
        return new HashSet<>(data);
    }

    @Before
    public void setup() {
        t1 = DateTime.now(DateTimeZone.UTC);
        t0 = t1.minus(Duration.standardMinutes(1)).minus(Duration.standardSeconds(2));
        tsv_old_keep = new MutableTimeSeriesValue(t0, GroupName.valueOf("keep"), singletonMap(MetricName.valueOf("value"), MetricValue.fromStrValue("tsv_old_keep")));
        tsv_old_overwrite = new MutableTimeSeriesValue(t0, GroupName.valueOf("overwrite"), singletonMap(MetricName.valueOf("value"), MetricValue.fromStrValue("tsv_old_overwrite")));
        tsv_new = new MutableTimeSeriesValue(t1, GroupName.valueOf("new"), singletonMap(MetricName.valueOf("value"), MetricValue.fromStrValue("tsv_new")));
        tsv_new_overwrite = new MutableTimeSeriesValue(t1, GroupName.valueOf("overwrite"), singletonMap(MetricName.valueOf("value"), MetricValue.fromStrValue("tsv_new_overwrite")));

        expected = Arrays.asList(tsv_old_keep, tsv_new, tsv_new_overwrite);
        ts_data_0 = new MutableTimeSeriesCollection(t0, Stream.concat(Stream.of(tsv_old_keep), Stream.of(tsv_old_overwrite)));
        ts_data_1 = new MutableTimeSeriesCollection(t1, Stream.concat(Stream.of(tsv_new_overwrite), Stream.of(tsv_new)));
    }

    @Test
    public void combined() {
        BackRefTimeSeriesCollection ts_data = new BackRefTimeSeriesCollection();
        ts_data.merge(ts_data_0);
        ts_data.merge(ts_data_1);

        assertEquals(t1, ts_data.getTimestamp());
        assertThat(ts_data.getTSValues().stream().collect(Collectors.toList()),
                CoreMatchers.hasItems(expected.stream().toArray(TimeSeriesValue[]::new)));
    }
}
