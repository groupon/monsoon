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
package com.groupon.lex.metrics.timeseries.expression;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.expression.GroupExpression;
import com.groupon.lex.metrics.expression.LiteralGroupExpression;
import com.groupon.lex.metrics.timeseries.ExpressionLookBack;
import com.groupon.lex.metrics.timeseries.MutableTimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.MutableTimeSeriesValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollectionPair;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollectionPairInstance;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricDeltaSet;
import com.groupon.lex.metrics.transformers.LiteralNameResolver;
import static java.util.Collections.singletonMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static org.hamcrest.CoreMatchers.hasItem;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class MetricSelectorTest {
    private static final GroupExpression group = new LiteralGroupExpression(new LiteralNameResolver("99", "Luftballons"));
    private static final GroupExpression notfound_group = new LiteralGroupExpression(new LiteralNameResolver("99", "Kriegsminister"));
    private static final DateTime t0 = new DateTime(2015, 12, 23, 0, 0, 1, DateTimeZone.UTC);
    private Context ctx;

    @Before
    public void setup() {
        final MutableTimeSeriesCollection previous = new MutableTimeSeriesCollection(t0.minus(Duration.standardMinutes(1)));
        final MutableTimeSeriesCollection current = new MutableTimeSeriesCollection(t0, Stream.of(new MutableTimeSeriesValue(GroupName.valueOf("99", "Luftballons"), singletonMap(MetricName.valueOf("value"), MetricValue.fromIntValue(17)))));
        final TimeSeriesCollectionPair ts_data = new TimeSeriesCollectionPairInstance(t0.minus(Duration.standardMinutes(2))) {
            {
                startNewCycle(previous.getTimestamp(), ExpressionLookBack.EMPTY);
                previous.getData().values().forEach(this.getCurrentCollection()::add);
                startNewCycle(current.getTimestamp(), ExpressionLookBack.EMPTY);
                current.getData().values().forEach(this.getCurrentCollection()::add);
            }
        };

        ctx = new SimpleContext(ts_data, (alert) -> {
        });
    }

    @Test
    public void lookup() {
        final MetricSelector selector = new MetricSelector(group, MetricName.valueOf("value"));
        TimeSeriesMetricDeltaSet found = selector.apply(ctx);

        assertThat(found.streamValues().collect(Collectors.toList()),
                hasItem(MetricValue.fromIntValue(17)));
    }

    @Test
    public void metric_notfound() {
        final MetricSelector selector = new MetricSelector(group, MetricName.valueOf("99 Kriegsminister"));
        TimeSeriesMetricDeltaSet found = selector.apply(ctx);

        assertTrue(found.isEmpty());
    }

    @Test
    public void group_notfound() {
        final MetricSelector selector = new MetricSelector(notfound_group, MetricName.valueOf("99 Kriegsminister"));
        TimeSeriesMetricDeltaSet found = selector.apply(ctx);

        assertTrue(found.isEmpty());
    }
}
