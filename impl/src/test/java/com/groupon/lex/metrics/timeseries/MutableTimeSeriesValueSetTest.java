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
import static java.util.Collections.singletonMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.Matchers.hasEntry;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class MutableTimeSeriesValueSetTest {
    private static final MetricName metric_key = MetricName.valueOf("value");
    private static final MetricValue metric_value = MetricValue.fromIntValue(7);
    private static final DateTime t0 = DateTime.now(DateTimeZone.UTC);
    private static final GroupName group_name = GroupName.valueOf("foo", "bar");
    private static final SimpleGroupPath group_path = SimpleGroupPath.valueOf("foo", "bar");
    private static final Map<MetricName, MetricValue> values = singletonMap(metric_key, metric_value);
    private static final TimeSeriesValue tsv0 = new MutableTimeSeriesValue(group_name, values);
    private final static TimeSeriesValue tsv1 = new MutableTimeSeriesValue(GroupName.valueOf(group_name.getPath(), singletonMap("x", MetricValue.TRUE)), values);

    @Test
    public void empty_constructor() {
        TimeSeriesValueSet tsv_set = new TimeSeriesValueSet(Stream.empty());

        assertTrue(tsv_set.asMap().isEmpty());
        assertTrue(tsv_set.findMetric(metric_key).isEmpty());
        assertTrue(tsv_set.getPaths().isEmpty());

        assertTrue(tsv_set.stream(group_path).collect(Collectors.toList()).isEmpty());
    }

    @Test
    public void constructor() {
        TimeSeriesValueSet tsv_set = new TimeSeriesValueSet(Stream.of(tsv0, tsv1));

        assertThat(tsv_set.asMap(), hasEntry(equalTo(group_path), hasItem(tsv0)));
        assertThat(tsv_set.asMap(), hasEntry(equalTo(group_path), hasItem(tsv1)));
        assertThat(tsv_set.findMetric(metric_key).streamAsMap().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
                hasEntry(group_name.getTags(), metric_value));
        assertThat(tsv_set.getPaths(), hasItem(group_path));

        assertThat(tsv_set.stream(group_path).collect(Collectors.toList()),
                hasItem(tsv0));
    }
}
